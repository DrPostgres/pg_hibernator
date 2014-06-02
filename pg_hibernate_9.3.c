#include "postgres.h"

#if PG_VERSION_NUM >= 90300 && PG_VERSION_NUM < 90400

#include "pg_hibernator.h"

PG_MODULE_MAGIC;

/*
 * Overall flow of control:
 *
 * _PG_init() registers a BGWorker for BufferSaver process, and one BGWorker
 * each for the save-files it finds in $PGDATA/pg_hibernator/.
 *
 * On shutdown request, the BufferSaver scans the shared buffers and saves the
 * list of blocks currently in memory to the $PGDATA/pg_hibernator/ directory;
 * one save-file for each database.
 *
 * When launched, the BlockReader reads the save-file assigned to it, connects
 * to the database represented by that save-file, and restores the blocks
 * identified by the list of blocks in save-file.
 *
 * Database numbers (and hence save-files with names) 0 and 1 are reserved;
 * In _PG_init() 0 is used to identify and register the BufferSaver, and 1 is
 * reserved in BufferSaver for save-file that contains global objects.
 */

typedef struct SharedState
{
	LWLockId	lock;
} SharedState;

static void		SharedStateSetup(void);
static void		shmem_startup(void);
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static SharedState *shared_mem = NULL;

typedef struct SavedBuffer
{
	Oid			database;
	Oid			filenode;	/* On-disk marker: 'r', for Relfilenode */
	ForkNumber	forknum;	/* On-disk marker: 'f' */
	BlockNumber	blocknum;	/* On-disk marker: 'b' */
							/* On-disk marker: 'N', for range of N blocks */
} SavedBuffer;

/* Primary functions */
void			_PG_init(void);
static void		DefineGUCs(void);
static void		CreateDirectory(void);

static void		RegisterBlockReaders(void);
static bool		RegisterWorker(int id);

static void		BlockReaderMain(Datum main_arg);
static void		ReadBlocks(int filenum);

static void		BufferSaverMain(Datum main_arg);
static void		SaveBuffers(void);

/* Secondary/supporting functions */
static void		sigtermHandler(SIGNAL_ARGS);
static void		sighupHandler(SIGNAL_ARGS);

static void		WorkerCommon(void);
static int		SavedBufferCmp(const void *a, const void *b);
static Oid		GetRelOid(Oid filenode);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static bool		guc_enabled = true;					/* Is the extension enabled? */
static bool		guc_parallel_enabled = false;		/* Can we restore databases in parallel? */
static char*	guc_default_database = "postgres";	/* Default DB to connect to. */

/*
 * Signal handler for SIGTERM
 *
 * Set a flag to notify the main loop of the signal received, and set our latch
 * to wake it up.
 */
static void
sigtermHandler(SIGNAL_ARGS)
{
	int	save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *
 * Used only in BufferSaver. Set a flag to notify the main loop of the signal
 * received, and set our latch to wake it up.
 */
static void
sighupHandler(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

/* This extension's entry point. */
void
_PG_init(void)
{
	SharedStateSetup();
	DefineGUCs();
	CreateDirectory();
	/*
	 * Create the BufferSaver irrespective of whether the extension is enabled.
	 * The BufferSaver will check the parameter when it receives SIGTERM, and act
	 * accordingly. This way the user can start the server with the extension
	 * disabled (pg_hibernator.enabled=false), enable the extension while
	 * server is running, and expect the save-files to be created when the server
	 * shuts down.
	 */
	 /* Register the BufferSaver worker */
	RegisterWorker(0);

	/*
	 * In Postgres version 9.4 and above, we use the dynamic background worker
	 * infrastructure for BlockReaders, and the BufferSaver process does the
	 * legwork of registering the BlockReader workers.
	 */
	/*
	 * In Postgres 9.3 we use regular workers for BlockReader processes, and
	 * they are registered right here.
	 */
	RegisterBlockReaders(); /* Register the BlockReader worker processes */
}

static void
SharedStateSetup()
{
	/* Request one LWLock */
	RequestAddinLWLocks(1);

	/* Register our hook for Shared Memory initialization */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = shmem_startup;
}

static void
shmem_startup()
{
	bool found;

    /* reset in case this is a restart within the postmaster */
    shared_mem = NULL;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared_mem = ShmemInitStruct("pg_hibernator",
                           sizeof(SharedState),
                           &found);
    if (!found)
    {
        /* First time through */
        shared_mem->lock = LWLockAssign();
    }

    LWLockRelease(AddinShmemInitLock);
}

/* Declare the parameters */
static void
DefineGUCs(void)
{
	DefineCustomBoolVariable("pg_hibernator.enabled",
							"Enable/disable automatic hibernation.",
							NULL,
							&guc_enabled,
							guc_enabled,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_hibernator.parallel",
							"Enable/disable restoring databases in parallel.",
							NULL,
							&guc_parallel_enabled,
							guc_parallel_enabled,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("pg_hibernator.default_database",
							"Database to connect to, by default.",
							"Postgres Hibernator will connect to this database when saving buffers, and when reading blocks of global objects.",
							&guc_default_database,
							guc_default_database,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
}

/*
 * Create the directory to save files in, if it doesn't already exist.
 */
static void
CreateDirectory(void)
{
	struct stat		st;
	const char	   *hibernate_dir = SAVE_LOCATION;

	if (stat(hibernate_dir, &st) == 0)
	{
		/* Is it not a directory? */
		if (!S_ISDIR(st.st_mode))
			ereport(ERROR,
					(errmsg("\"%s\" exists but is not a directory, hence disabling hibernation",
							hibernate_dir)));
	}
	else
	{
		/* Directory does not exist? */
		if (errno == ENOENT)
		{
			/* Create directory */
			if (mkdir(hibernate_dir, S_IRWXU) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
								hibernate_dir)));
		}
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat directory \"%s\": %m", hibernate_dir)));
	}

	/* XXX: Should we make sure we have write permissions on this directory? */
}

static void
RegisterBlockReaders(void)
{
	DIR			   *dir;
	const char	   *hibernate_dir;
	struct dirent   *dent;

	/* Don't create BlockReaders if the extension is disabled. */
	if (!guc_enabled)
		return;

	hibernate_dir = SAVE_LOCATION;

	dir = opendir(hibernate_dir);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", hibernate_dir)));

	errno = 0;
	while ((dent = readdir(dir)) != NULL)
	{
		int		filenum;

		/* Skip worker creation if we can't parse the file name. */
		if (!parseSavefileName(dent->d_name, &filenum))
			continue;

		RegisterWorker(filenum);
	}

	if (errno != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error encountered during readdir \"%s\": %m", hibernate_dir)));

	closedir(dir);
}

static bool
RegisterWorker(int id)
{
	BackgroundWorker	worker;

	/*
	 * Protect against the possibility of more members being added to the
	 * structure than we are using below.
	 */
	MemSet(&worker, 0, sizeof(worker));

	worker.bgw_main_arg = Int32GetDatum(id);
	worker.bgw_flags		= BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	if (id == 0)
	{
		/* Register the BufferSaver background worker */
		worker.bgw_start_time	= BgWorkerStart_ConsistentState;
		worker.bgw_restart_time	= 0;	/* Keep the BufferSaver running */
		worker.bgw_main			= BufferSaverMain;
		snprintf(worker.bgw_name, BGW_MAXLEN, "Buffer Saver");
	}
	else
	{
		/* Register a BlockReader background worker process */
		worker.bgw_start_time	= BgWorkerStart_ConsistentState;
		worker.bgw_restart_time	= BGW_NEVER_RESTART;	/* Don't restart BlockReaders upon error */
		worker.bgw_main			= BlockReaderMain;
		snprintf(worker.bgw_name, BGW_MAXLEN, "Block Reader %d", id);
	}

	/*
	 * This API is deficient as it can't report failure; it just does an
	 * ereport(LOG) on failure.
	 */
	RegisterBackgroundWorker(&worker);
	return true;
}

static void
WorkerCommon(void)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, sighupHandler);
	pqsignal(SIGTERM, sigtermHandler);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();
}

static void
BlockReaderMain(Datum main_arg)
{
	int					id = DatumGetInt32(main_arg);
	DIR				   *dir;
	const char		   *hibernate_dir = SAVE_LOCATION;
	struct dirent	   *dent;
	int					filenum;

	WorkerCommon();

	dir = opendir(hibernate_dir);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("Block Reader %d: could not open directory \"%s\": %m",
						id, hibernate_dir)));

	/*
	 * Reset errno before making system call, so that we don't trip over an
	 * error that occurred earlier.
	 */
	errno = 0;

	/* Scan the directory looking for file this worker is assigned to. */
	while ((dent = readdir(dir)) != NULL)
	{
		if (!parseSavefileName(dent->d_name, &filenum))
			continue;

		/* Stop if this is the file assigned to this worker. */
		if (filenum == id)
			break;
	}

	if (dent == NULL)
	{
		closedir(dir);

		if (errno != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("Block Reader %d: encountered error during readdir \"%s\": %m",
							filenum, hibernate_dir)));

		ereport(ERROR,
				(errmsg("Block Reader %d: could not find its save-file", filenum)));
	}
	closedir(dir);

	/* We found the file we're supposed to restore. */

	/* If parallelism is disabled, wait until it's our turn to read blocks. */

	/*
	 * Despite the warning on LWLockAcquire() that an LWLock should not be held
	 * for more than a few seconds, because holding one causes the query-cancel
	 * and die signals are blocked, I think it's safe to hold an LWLock around
	 * a database buffer restore operation since we're a background worker and
	 * query-cancel is intended for regular backends, and die() (a.k.a SIGTERM)
	 * is handled by us, and we promptly honor it.
	 */
	if (!guc_parallel_enabled)
		LWLockAcquire(shared_mem->lock, LW_EXCLUSIVE);

	ReadBlocks(filenum);

	if (!guc_parallel_enabled)
		LWLockRelease(shared_mem->lock);

	/*
	 * Exit with non-zero status to ensure that this worker is not restarted.
	 *
	 * For any backend connected to shared-buffers, an exit code other than 0 or 1
	 * causes a system-wide restart, so we have no choice but to use 1. Since an
	 * ERROR also causes exit code 1, it would've been nice if we could use some
	 * other code to signal normal exit, so that a monitor could differentiate
	 * between a successful exit, and an exit due to an ERROR.
	 *
	 * To get around this ambiguity we resort to logging a message to server log;
	 * this message should console the user that everything went okay, even though
	 * the exit code is 1.
	 */
	ereport(LOG, (errmsg("Block Reader %d: all blocks read successfully", filenum)));
	proc_exit(1);
}

static void
ReadBlocks(int filenum)
{
	FILE	   *file;
	char		record_type;
	char	   *dbname;
	Oid			record_filenode;
	ForkNumber	record_forknum;
	BlockNumber	record_blocknum;
	BlockNumber	record_range;

	int			log_level		= DEBUG3;
	Oid			relOid			= InvalidOid;
	Relation	rel				= NULL;
	bool		skip_relation	= false;
	bool		skip_fork		= false;
	bool		skip_block		= false;
	BlockNumber	nblocks			= 0;
	BlockNumber	blocks_restored	= 0;
	const char *filepath;

	/*
	 * If this condition changes, then this code, and the code in the writer
	 * will need to be changed; especially the format specifiers in log and
	 * error messages.
	 */
	StaticAssertStmt(MaxBlockNumber == 0xFFFFFFFE, "Code may need review.");

	filepath = getSavefileName(filenum);
	file = fileOpen(filepath, PG_BINARY_R);
	dbname = readDBName(file, filepath);

	/*
	 * When restoring global objects, the dbname is zero-length string, and non-
	 * zero length otherwise. And filenum is never expected to be smaller than 1.
	 */
	Assert(filenum >= 1);
	Assert(filenum == 1 ? strlen(dbname) == 0 : strlen(dbname) > 0);

	/* To restore the global objects, use default database */
	BackgroundWorkerInitializeConnection(filenum == 1 ? guc_default_database : dbname, NULL);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "restoring buffers");

	/*
	 * Note that in case of a read error, we will leak relcache entry that we may
	 * currently have open. In case of EOF, we close the relation after the loop.
	 */
	while (fileRead(&record_type, 1, file, true, filepath))
	{
		/*
		 * If we want to process the signals, this seems to be the best place
		 * to do it. Generally the backends refrain from processing config file
		 * while in transaction, but that's more for the fear of allowing GUC
		 * changes to affect expression evaluation, causing different results
		 * for the same expression in a transaction. Since this worker is not
		 * processing any queries, it is okay to process the config file here.
		 *
		 * Even though it's okay to process SIGHUP here, doing so doesn't add
		 * any value. The only reason we might want to process config file here
		 * would be to allow the user to interrupt the BlockReader's operation
		 * by changing this extenstion's GUC parameter. But the user can do that
		 * anyway, using SIGTERM or pg_terminate_backend().
		 */

		/* Stop processing the save-file if the Postmaster wants us to die. */
		if (got_sigterm)
			break;

		ereport(log_level,
				(errmsg("record type %x - %c", record_type, record_type)));

		switch (record_type)
		{
			case 'r':
			{
				/* Close the previous relation, if any. */
				if (rel)
				{
					relation_close(rel, AccessShareLock);
					rel = NULL;
				}

				record_forknum = InvalidForkNumber;
				record_blocknum = InvalidBlockNumber;
				nblocks = 0;

				fileRead(&record_filenode, sizeof(Oid), file, false, filepath);

				relOid = GetRelOid(record_filenode);

				ereport(log_level, (errmsg("processing filenode %u, relation %u",
										record_filenode, relOid)));
				/*
				 * If the relation has been rewritten/dropped since we saved it,
				 * just skip it and process the next relation.
				 */
				if (relOid == InvalidOid)
					skip_relation = true;
				else
				{
					skip_relation = false;

					/* Open the relation */
					rel = relation_open(relOid, AccessShareLock);
					RelationOpenSmgr(rel);
				}
			}
			break;
			case 'f':
			{
				record_blocknum = InvalidBlockNumber;
				nblocks = 0;

				fileRead(&record_forknum, sizeof(ForkNumber), file, false, filepath);

				if (skip_relation)
					continue;

				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a fork record without a preceeding relation record")));

				ereport(log_level, (errmsg("processing fork %d", record_forknum)));

				if (!smgrexists(rel->rd_smgr, record_forknum))
					skip_fork = true;
				else
				{
					skip_fork = false;

					nblocks = RelationGetNumberOfBlocksInFork(rel, record_forknum);
				}
			}
			break;
			case 'b':
			{
				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errmsg("found a block record without a preceeding fork record")));

				fileRead(&record_blocknum, sizeof(BlockNumber), file, false, filepath);

				if (skip_relation || skip_fork)
					continue;

				/*
				 * Don't try to read past the file; the file may have been shrunk
				 * by a vaccum/truncate operation.
				 */
				if (record_blocknum >= nblocks)
				{
					ereport(log_level,
							(errmsg("reader %d skipping block filenode %u forknum %d blocknum %u",
									filenum, record_filenode, record_forknum, record_blocknum)));

					skip_block = true;
					continue;
				}
				else
				{
					Buffer	buf;

					skip_block = false;

					ereport(log_level,
							(errmsg("reader %d reading block filenode %u forknum %d blocknum %u",
									filenum, record_filenode, record_forknum, record_blocknum)));

					buf = ReadBufferExtended(rel, record_forknum, record_blocknum, RBM_NORMAL, NULL);
					ReleaseBuffer(buf);

					++blocks_restored;
				}
			}
			break;
			case 'N':
			{
				BlockNumber block;

				Assert(record_blocknum != InvalidBlockNumber);

				if (record_blocknum == InvalidBlockNumber)
					ereport(ERROR,
							(errmsg("found a block range record without a preceeding block record")));

				fileRead(&record_range, sizeof(int), file, false, filepath);

				if (skip_relation || skip_fork || skip_block)
					continue;

				ereport(log_level,
						(errmsg("reader %d reading range filenode %u forknum %d blocknum %u range %u",
								filenum, record_filenode, record_forknum, record_blocknum, record_range)));

				for (block = record_blocknum + 1; block <= (record_blocknum + record_range); ++block)
				{
					Buffer	buf;

					/*
					* Don't try to read past the file; the file may have been
					* shrunk by a vaccum operation.
					*/
					if (block >= nblocks)
					{
						ereport(log_level,
								(errmsg("reader %d skipping block range filenode %u forknum %d start %u end %u",
										filenum, record_filenode, record_forknum,
										block, record_blocknum + record_range)));

						break;
					}

					buf = ReadBufferExtended(rel, record_forknum, block, RBM_NORMAL, NULL);
					ReleaseBuffer(buf);

					++blocks_restored;
				}
			}
			break;
			default:
			{
				ereport(ERROR,
						(errmsg("found unexpected save-file marker %x - %c)", record_type, record_type)));
				Assert(false);
			}
			break;
		}
	}

	if (rel)
		relation_close(rel, AccessShareLock);

	ereport(LOG,
			(errmsg("Block Reader %d: restored %u blocks",
					filenum, blocks_restored)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	fileClose(file, filepath);

	/* Remove the save-file */
	if (remove(filepath) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error removing file \"%s\" : %m", filepath)));
}

static void
BufferSaverMain(Datum main_arg)
{
	WorkerCommon();

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int	rc;

		ResetLatch(&MyProc->procLatch);

		/*
		 * Wait on the process latch, which sleeps as necessary, but is awakened
		 * if postmaster dies. This way the background process goes away
		 * immediately in case of an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10 * 1000L);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	/*
	 * We recieved the SIGTERM; Shutdown is in progress, so save the
	 * shared-buffer contents.
	 */

	/* Save the buffers only if the extension is enabled. */
	if (guc_enabled)
		SaveBuffers();

	/*
	 * The worker exits here. A proc_exit(0) is not necessary, we'll let the
	 * caller do that.
	 */
}

static void
SaveBuffers(void)
{
	int						i;
	int						num_buffers;
	int						log_level		= DEBUG3;
	SavedBuffer			   *saved_buffers;
	volatile BufferDesc	   *bufHdr;			// XXX: Do we really need volatile here?
	FILE				   *file			= NULL;
	int						database_counter= 0;
	Oid						prev_database	= InvalidOid;
	Oid						prev_filenode	= InvalidOid;
	ForkNumber				prev_forknum	= InvalidForkNumber;
	BlockNumber				prev_blocknum	= InvalidBlockNumber;
	BlockNumber				range_counter	= 0;
	const char			   *savefile_path;

	/*
	 * XXX: If the memory request fails, ask for a smaller memory chunk, and use
	 * it to create chunks of save-files, and make the workers read those chunks.
	 *
	 * This is not a concern as of now, so deferred; there's at least one other
	 * place that allocates (NBuffers * (much_bigger_struct)), so this seems to
	 * be an acceptable practice.
	 */

	saved_buffers = (SavedBuffer *) palloc(sizeof(SavedBuffer) * NBuffers);

	/* Lock the buffer partitions for reading. */
	for (i = 0; i < NUM_BUFFER_PARTITIONS; ++i)
		LWLockAcquire(FirstBufMappingLock + i, LW_SHARED);

	/* Scan and save a list of valid buffers. */
	for (num_buffers = 0, i = 0, bufHdr = BufferDescriptors; i < NBuffers; ++i, ++bufHdr)
	{
		/* Lock each buffer header before inspecting. */
		LockBufHdr(bufHdr);

		/* Skip invalid buffers */
		if ((bufHdr->flags & BM_VALID) && (bufHdr->flags & BM_TAG_VALID))
		{
			saved_buffers[num_buffers].database	= bufHdr->tag.rnode.dbNode;
			saved_buffers[num_buffers].filenode	= bufHdr->tag.rnode.relNode;
			saved_buffers[num_buffers].forknum	= bufHdr->tag.forkNum;
			saved_buffers[num_buffers].blocknum	= bufHdr->tag.blockNum;

			++num_buffers;
		}

		UnlockBufHdr(bufHdr);
	}

	/* Unlock the buffer partitions in reverse order, to avoid a deadlock. */
	for (i = NUM_BUFFER_PARTITIONS - 1; i >= 0; --i)
		LWLockRelease(FirstBufMappingLock + i);

	/*
	 * Sort the list, so that we can optimize the storage of these buffers.
	 *
	 * The side-effect of this storage optimization is that when reading the
	 * blocks back from relation forks, it leads to sequential reads, which
	 * improve the restore speeds quite considerably as compared to random reads
	 * from different blocks all over the data directory.
	 */
	pg_qsort(saved_buffers, num_buffers, sizeof(SavedBuffer), SavedBufferCmp);

	/* Connect to the database and start a transaction for database name lookups. */
	BackgroundWorkerInitializeConnection(guc_default_database, NULL);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "saving buffers");

	for (i = 0; i < num_buffers; ++i)
	{
		int j;
		SavedBuffer *buf = &saved_buffers[i];

		if (i == 0)
		{
			/*
			 * Special case for global objects. The sort brings them to the
			 * front of the list.
			 */

			/* Make sure the first buffer we save belongs to global object. */
			Assert(buf->database == InvalidOid);

			/*
			 * Database number (and save-file name) 1 is reserverd for storing
			 * list of buffers of global objects.
			 */
			database_counter = 1;

			savefile_path = getSavefileName(database_counter);
			file = fileOpen(savefile_path, PG_BINARY_W);
			writeDBName("", file, savefile_path);

			prev_database = buf->database;
		}

		if (buf->database != prev_database)
		{
			char *dbname;

			/*
			 * We are beginning to process a different database than the
			 * previous one; close the save-file of previous database, and open
			 * a new one.
			 */
			++database_counter;

			dbname = get_database_name(buf->database);

			Assert(dbname != NULL);

			if (file != NULL)
				fileClose(file, savefile_path);

			savefile_path = getSavefileName(database_counter);
			file = fileOpen(savefile_path, PG_BINARY_W);
			writeDBName(dbname, file, savefile_path);

			pfree(dbname);

			/* Reset trackers appropriately */
			prev_database	= buf->database;
			prev_filenode	= InvalidOid;
			prev_forknum	= InvalidForkNumber;
			prev_blocknum	= InvalidBlockNumber;
			range_counter	= 0;
		}

		if (buf->filenode != prev_filenode)
		{
			/* We're beginning to process a new relation; emit a record for it. */
			fileWrite("r", 1, file, savefile_path);
			fileWrite(&(buf->filenode), sizeof(Oid), file, savefile_path);

			/* Reset trackers appropriately */
			prev_filenode	= buf->filenode;
			prev_forknum	= InvalidForkNumber;
			prev_blocknum	= InvalidBlockNumber;
			range_counter	= 0;
		}

		if (buf->forknum != prev_forknum)
		{
			/*
			 * We're beginning to process a new fork of this relation; add a
			 * record for it.
			 */
			fileWrite("f", 1, file, savefile_path);
			fileWrite(&(buf->forknum), sizeof(ForkNumber), file, savefile_path);

			/* Reset trackers appropriately */
			prev_forknum	= buf->forknum;
			prev_blocknum	= InvalidBlockNumber;
			range_counter	= 0;
		}

		ereport(log_level,
				(errmsg("writer: writing block db %d filenode %d forknum %d blocknum %d",
						database_counter, prev_filenode, prev_forknum, buf->blocknum)));

		fileWrite("b", 1, file, savefile_path);
		fileWrite(&(buf->blocknum), sizeof(BlockNumber), file, savefile_path);

		prev_blocknum = buf->blocknum;

		/*
		 * If a continuous range of blocks follows this block, then emit one
		 * entry for the range, instead of one for each block.
		 */
		range_counter = 0;

		for ( j = i+1; j < num_buffers; ++j)
		{
			SavedBuffer *tmp = &saved_buffers[j];

			if (tmp->database		== prev_database
				&& tmp->filenode	== prev_filenode
				&& tmp->forknum		== prev_forknum
				&& tmp->blocknum	== (prev_blocknum + range_counter + 1))
			{
				++range_counter;
			}
		}

		if (range_counter != 0)
		{
			ereport(log_level,
				(errmsg("writer: writing range db %d filenode %d forknum %d blocknum %d range %d",
						database_counter, prev_filenode, prev_forknum, prev_blocknum, range_counter)));

			fileWrite("N", 1, file, savefile_path);
			fileWrite(&range_counter, sizeof(range_counter), file, savefile_path);

			i += range_counter;
		}
	}

	ereport(LOG,
			(errmsg("Buffer Saver: saved metadata of %d blocks", num_buffers)));

	Assert(file != NULL);
	fileClose(file, savefile_path);

	pfree(saved_buffers);

	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

#define svdbfrcmp(fld)			\
	if (a->fld < b->fld)		\
		return -1;				\
	else if (a->fld > b->fld)	\
		return 1;

static int
SavedBufferCmp(const void *p, const void *q)
{
	SavedBuffer *a = (SavedBuffer *) p;
	SavedBuffer *b = (SavedBuffer *) q;

	svdbfrcmp(database);
	svdbfrcmp(filenode);
	svdbfrcmp(forknum);
	svdbfrcmp(blocknum);

	Assert(false);	// No two buffers should be storing identical page

	return 0;	// Keep compiler happy.
}

static Oid
GetRelOid(Oid filenode)
{
	int			ret;
	Oid			relid;
	bool		isnull;
	Datum		value[1] = { ObjectIdGetDatum(filenode) };

	static SPIPlanPtr	plan = NULL;

	/* If this is our first time here, create a plan and save it for later calls. */
	if (plan == NULL)
	{
		StringInfoData	buf;
		Oid	paramType[1] = {OIDOID};

		initStringInfo(&buf);
		appendStringInfo(&buf, "select oid from pg_class where pg_relation_filenode(oid) = $1");

		plan = SPI_prepare(buf.data, 1, (Oid*)&paramType);

		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %d", SPI_result);
	}

	ret = SPI_execute_plan(plan, (Datum*)&value, NULL, true, 1);

	if (ret != SPI_OK_SELECT)
		ereport(FATAL, (errmsg("SPI_execute_plan failed: error code %d", ret)));

	if (SPI_processed < 1)
		return InvalidOid;

	relid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		return InvalidOid;

	return relid;
}

#endif /* PG_VERSION_NUM == 90300 */
