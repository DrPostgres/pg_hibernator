#include "postgres.h"

#include <sys/stat.h>


/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* Header files needed by this plugin */
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

typedef struct SavedBuffer
{
	Oid			database;
	Oid			filenode;	/* Stream marker: 'r' */
	ForkNumber	forknum;	/* Stream marker: 'f' */
	BlockNumber	blocknum;	/* Stream marker: 'b' */
							/* Stream marker: 'N' for range of blocks */
} SavedBuffer;

/* Forward declarations */

#define SAVE_LOCATION "pg_hibernate"

/* Primary functions */
void			_PG_init(void);
static void		DefineGUCs(void);
static void		CreateDirectory(void);

static void		CreateWorkers(void);
static void		CreateWorker(int id);

static void		BlockReaderMain(Datum main_arg);
static void		ReadBlocks(int number, char *dbname);

static void		BufferSaverMain(Datum main_arg);
static void		SaveBuffers(void);

/* Secondary/supporting functions */
static void		sigtermHandler(SIGNAL_ARGS);
static void		sighupHandler(SIGNAL_ARGS);
static const char* getSaveDirName();
static const char* getDatabseSaveFileName(int number, const char* const dbname);
static void		WorkerCommon(void);
static int		SavedBufferCmp(const void *a, const void *b);
static Oid		GetRelOid(Oid filenode);
static bool		parseSaveFileName(const char *fname, int * number, char *dbname);

/*
 * TODO: Consider if all ereport(ERROR) calls should be converted to ereport(FATAL),
 * because the worker processes are not supposed to live beyond an error anyway.
 */

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static bool		hibernate_enabled = true;
static char*	default_database = "postgres";

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
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
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
sighupHandler(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

void
_PG_init(void)
{
	DefineGUCs();
	CreateDirectory();
	CreateWorkers();
}

static void
DefineGUCs(void)
{
	/* get the configuration */
	DefineCustomBoolVariable("hibernate.enable",
							"Enable/disable automatic hibernation.",
							NULL,
							&hibernate_enabled,
							true,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("hibernate.default_db",
							"Default database to connect to.",
							NULL,
							&default_database,
							default_database,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
}

/* We use static arrays in these functions, because the returned pointers */
static const char*
getSaveDirName()
{
	static char	hibernate_dir[sizeof(SAVE_LOCATION) + 1];

	if (hibernate_dir[0] == '\0')
		snprintf(hibernate_dir, sizeof(hibernate_dir), "%s", SAVE_LOCATION);

	return hibernate_dir;
}

static const char*
getDatabseSaveFileName(int number, const char *dbname)
{
	static char ret[MAXPGPATH];

	snprintf(ret, sizeof(ret), "%s/%d.%s.save", getSaveDirName(), number, dbname);

	return ret;
}

static bool
parseSaveFileName(const char *fname, int *number, char *dbname)
{
	int dbname_len;

	/* The save-file names are supposed to be in the format <integer>.<dbname>.save */

	if (sscanf(fname, "%d.%s", number, dbname) != 2)
		return false;	/* Fail if the name doesn't match the format we're expecting */

	dbname_len = strlen(dbname);

	if (strcmp(&dbname[dbname_len - 5], ".save") != 0)
		return false;	/* Fail if the name doesn't match the format we're expecting */

	dbname[dbname_len - 5] = '\0';

	return true;
}

static void
CreateDirectory(void)
{
	struct stat		st;
	const char	   *hibernate_dir = getSaveDirName();

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
}

static void
CreateWorkers(void)
{
	DIR			   *dir;
	const char	   *hibernate_dir;
	struct dirent   *dent;

	/*
	 * Create the buffer-saver irrespective of whether the extension is enabled. The
	 * buffer-saver will check the flag when it receives SIGTERM, and act accordingly.
	 * This is done so that the user can start the server with the extension disabled,
	 * enable the extension while server is running, and expect the save-files to be
	 * created when the server shuts down.
	 */
	CreateWorker(0);	/* Create the buffer-saver worker */

	/* Don't create block-readers if the extension is disabled. */
	if (!hibernate_enabled)
		return;

	hibernate_dir = getSaveDirName();

    dir = AllocateDir(hibernate_dir);
    if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", hibernate_dir)));

    while ((dent = ReadDir(dir, hibernate_dir)) != NULL)
    {
		int		number;
		char	dbname[NAMEDATALEN];

		if (!parseSaveFileName(dent->d_name, &number, dbname))
			continue;

		CreateWorker(number);
    }

	FreeDir(dir);
}

static void
CreateWorker(int id)
{
	BackgroundWorker	worker;

	if (id == 0)
	{
		worker.bgw_flags		= BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time	= BgWorkerStart_ConsistentState;
		worker.bgw_restart_time	= 0;	/* Keep the BufferSaver running */
		worker.bgw_main			= BufferSaverMain;
		snprintf(worker.bgw_name, BGW_MAXLEN, "Hibernate Buffer Saver");
	}
	else
	{
		worker.bgw_flags		= BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time	= BgWorkerStart_ConsistentState;
		worker.bgw_restart_time	= BGW_NEVER_RESTART;	/* Don't restart readers upon error */
		worker.bgw_main			= BlockReaderMain;
		snprintf(worker.bgw_name, BGW_MAXLEN, "Hibernate Block Reader %d", id);
	}

	worker.bgw_main_arg = Int32GetDatum(id);

	RegisterBackgroundWorker(&worker);
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
	int					index = DatumGetInt32(main_arg);
	DIR				   *dir;
	const char		   *hibernate_dir = getSaveDirName();
	struct dirent	   *dent;
	int					number;
	char				dbname[NAMEDATALEN];

	WorkerCommon();

	dir = AllocateDir(hibernate_dir);
    if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("block-reader worker %d could not open directory \"%s\": %m",
						index, hibernate_dir)));

    while ((dent = ReadDir(dir, hibernate_dir)) != NULL)
    {
		if (!parseSaveFileName(dent->d_name, &number, dbname))
			continue;

		if (number == index)
			break;
    }

    if (dent == NULL)
	{
		FreeDir(dir);
		ereport(ERROR,
				(errmsg("block-reader %d could not find its save-file", index)));
	}
	FreeDir(dir);

	/* We found the file we're supposed to restore. */
	ReadBlocks(number, dbname);

	/*
	 * Exit with non-zero status to ensure that this worker is not restarted.
	 *
	 * For any backend connected to shared-buffers, an exit code other than 0 or 1
	 * causes a system-wide restart, so we have no choice but to use 1. Since an
	 * ERROR also causes exit code 1, it would've been nice if we could use some
	 * other code to signal normal exit.
	 *
	 * To get around this limitation we resort to logging a message to server log;
	 * this message should console the user that everything went okay, even though
	 * the exit code is 1.
	 */
	ereport(LOG, (errmsg("block-reader %d read all blocks successfully", index)));
	proc_exit(1);
}

static void
ReadBlocks(int number, char *dbname)
{
	FILE	   *file;
	char		record_type;
	Oid			record_filenode;
	ForkNumber	record_forknum;
	BlockNumber	record_blocknum;
	int			record_range;

	int			log_level = DEBUG3;
	Oid			relOid;
	Relation	rel = NULL;
	bool		skip_relation = false;
	bool		skip_fork = false;
	bool		skip_block = false;
	int64		nblocks = 0;
	char	   *filename;

	filename = getDatabseSaveFileName(number, dbname);
	file = fopen(filename, PG_BINARY_R);
	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open save-file for database \"%s\": %m", dbname)));

	/* To restore the global objects, use default database */
	BackgroundWorkerInitializeConnection(number == 1 ? default_database : dbname, NULL);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "restoring buffers");

	while (!ferror(file))
	{
		/* If we want to process the signals, this seems to be the best place to do
		 * it. Generally the backends refrain from processing config file while in
		 * transaction, but that's more for the fear of allowing GUC changes to
		 * affect expression evaluation, causing different results in the same
		 * transaction. Since this worker is not processing any queries, it is okay
		 * to process the config file here.
		 *
		 * Even though it's okay to process SIGHUP here, doing so doesn't add any
		 * value. The only reason we might want to process config file here would
		 * be to allow the user to interrupt the block-reader's operation by
		 * changing this extenstion's GUC parameter. But the user can do that
		 * anyway, using SIGTERM or pg_terminate_backend().
		 */

		/* Stop processing the save-file if the Postmaster wants us to die. */
		if (got_sigterm)
			break;

		if (fread(&record_type, 1, 1, file) != 1)
		{
			/* Most likely it's because we reached end-of-file */

			if (rel)
			{
				relation_close(rel, AccessShareLock);
				rel = NULL;
			}

			/*
			 * If it is feof() we're good, but if it is ferror(), the check after
			 * the loop will take care of it.
			 */
			break;
		}

		switch (record_type)
		{
			case 'r':
			{
				if (rel)
				{
					relation_close(rel, AccessShareLock);
					rel = NULL;
				}

				nblocks = 0;
				record_forknum = InvalidForkNumber;
				record_blocknum = InvalidBlockNumber;

				if (fread(&record_filenode, sizeof(Oid), 1, file) != 1)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("error reading save-file of \"%s\" : %m", dbname)));

				relOid = GetRelOid(record_filenode);

				ereport(log_level, (errmsg("processing filenode %d, relation %d",
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

				if (skip_relation)
					continue;

				Assert(rel != NULL);
				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a fork marker without a preceeding relation marker")));

				if (fread(&record_forknum, sizeof(ForkNumber), 1, file) != 1)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("error reading save-file of \"%s\" : %m", dbname)));

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
				if (skip_relation || skip_fork)
					continue;

				Assert(rel != NULL);
				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a block marker without a preceeding relation marker")));

				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errmsg("found a block marker without a preceeding fork marker")));

				if (fread(&record_blocknum, sizeof(BlockNumber), 1, file) != 1)
				{
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("error reading save-file of \"%s\" : %m", dbname)));
				}
				ereport(log_level, (errmsg("processing block %d", record_blocknum)));

				/*
				 * Don't try to read past the file; the file may have been shrunk
				 * by a vaccum operation.
				 */
				if (record_blocknum >= nblocks)
				{
					skip_block = true;
					continue;
				}
				else
				{
					Buffer      buf;

					skip_block = false;

					buf = ReadBufferExtended(rel, record_forknum, record_blocknum, RBM_NORMAL, NULL);
					ReleaseBuffer(buf);
				}
			}
			break;
			case 'N':
			{
				int64 block;

				if (skip_relation || skip_fork || skip_block)
					continue;

				Assert(record_blocknum != InvalidBlockNumber);
				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a block range marker without a preceeding relation marker")));

				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errmsg("found a block range marker without a preceeding fork marker")));

				if (record_blocknum == InvalidBlockNumber)
					ereport(ERROR,
							(errmsg("found a block range marker without a preceeding block marker")));

				if (fread(&record_range, sizeof(int), 1, file) != 1)
				{
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("error reading save-file of \"%s\" : %m", dbname)));
				}
				ereport(log_level, (errmsg("processing range %d", record_range)));

				for (block = record_blocknum + 1; block < (record_blocknum + record_range); ++block)
				{
					Buffer      buf;

					buf = ReadBufferExtended(rel, record_forknum, block, RBM_NORMAL, NULL);
					ReleaseBuffer(buf);
				}
			}
			break;
			default:
			{
				Assert(false);
				ereport(ERROR,
						(errmsg("found unexpected save-file marker %x - %c)", record_type, record_type)));
			}
			break;
		}
	}
	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error reading save-file of \"%s\" : %m", dbname)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	fclose(file);

	/* Remove the save-file */
	if (!remove(filename))
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error removing save-file of \"%s\" : %m", dbname)));
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

		/*
		 * Wait on the process latch, which sleeps as necessary, but is awakened if
		 * postmaster dies.  This way the background process goes away immediately
		 * in case of an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10 * 1000L);
		ResetLatch(&MyProc->procLatch);

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

	/* We recieved the SIGTERM, so save the shared-buffer contents */

	/* Save the buffers only if the extension is enabled. */
	if (hibernate_enabled)
		SaveBuffers();

	/* The worker exits here. A proc_exit(0) is not necessary, let the caller do that. */
}

static void
SaveBuffers(void)
{
	int					i;
	int					num_buffers;
	SavedBuffer		   *saved_buffers;
	volatile BufferDesc *bufHdr;			// XXX: Do we really need volatile here?
	FILE			   *file			= NULL;
	int					database_number;
	Oid					prev_database	= InvalidOid;
	Oid					prev_filenode	= InvalidOid;
	ForkNumber			prev_forknum	= InvalidForkNumber;
	BlockNumber			prev_blocknum	= InvalidBlockNumber;
	uint				range_counter	= 0;

	/*
	 * TODO: If the memory request fails, ask for a smaller memory chunk, and use it
	 * to create chunks of save-files, and make the workers read those chunks.
	 */

	saved_buffers = (SavedBuffer *) palloc(sizeof(SavedBuffer) * NBuffers);

	/* Lock the buffer partitions */
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
			saved_buffers[i].database	= bufHdr->tag.rnode.dbNode;
			saved_buffers[i].filenode	= bufHdr->tag.rnode.relNode;
			saved_buffers[i].forknum		= bufHdr->tag.forkNum;
			saved_buffers[i].blocknum	= bufHdr->tag.blockNum;

			++num_buffers;
		}

		UnlockBufHdr(bufHdr);
	}

	/*
	 * Unlock the buffer partitions in reverse order, to avoid deadlock. Although
	 * this function is called during shutdown, and one would expect us to be alone,
	 * but that may not be true, as other user/worker backends may still be alive,
	 * just as we are. Also, it doesn't hurt to follow the protocol.
	 */
	for (i = NUM_BUFFER_PARTITIONS; --i >= 0;)
		LWLockRelease(FirstBufMappingLock + i);

	/* Sort the buffers, so that we can optimize the storage of these buffers. */
	pg_qsort(saved_buffers, num_buffers, sizeof(SavedBuffer), SavedBufferCmp);

	/*
	 * Number 0 is reserved; In CreateWorkers(), 0 is used to identify and register
	 * the BufferSaver. And number 1 is reserved for save-file that contains global
	 * objects.
	 */
	database_number = 1;

	/*
	 * TODO: Figure out why we need a default DB here. The autovacuum-launcher
	 * seems to do fine without it! See InitPostgres(NULL, InvalidOid, NULL, NULL)
	 * call in autovacuum.c
	 */
	BackgroundWorkerInitializeConnection(default_database, NULL);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "saving buffers");

	for (i = 0; i < num_buffers; ++i)
	{
		SavedBuffer *buf = &saved_buffers[i];

		if (buf->database == 0)
		{
			/* Special case for global objects */

			file = AllocateFile(getDatabseSaveFileName(database_number, "global"), PG_BINARY_W);

			if (file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						errmsg("could not create save-file for global objects : %m")));
		}
		else if (buf->database != prev_database)
		{
			char *dbname;

			/*
			 * We are beginning to process a different database than the previous one;
			 * close the save-file of previous database, and open a new one.
			 */
			++database_number;

			dbname = get_database_name(buf->database);

			Assert(dbname != NULL);

			/*
			 * Ensure that this is not the first database we process. This assertion
			 * is not really necessary, but it cements our assumption that the qsort
			 * above brings the global objects to the front.
			 */
			Assert(file != NULL);
			FreeFile(file);
			file = NULL;
			file = AllocateFile(getDatabseSaveFileName(database_number, dbname), PG_BINARY_W);

			if (file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						errmsg("could not create save-file for database \"%s\" : %m", dbname)));

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
			/* We're beginning to process a new relation; emit a marker for it. */
			fwrite("r", 1, 1, file);
			fwrite(&(buf->filenode), sizeof(Oid), 1, file);

			/* Reset trackers appropriately */
			prev_filenode	= buf->filenode;
			prev_forknum	= InvalidForkNumber;
			prev_blocknum	= InvalidBlockNumber;
			range_counter	= 0;
		}

		if (buf->forknum != prev_forknum)
		{
			/*
			 * We're beginning to process a new fork of this relation; add a marker
			 * for it.
			 */
			fwrite("f", 1, 1, file);
			fwrite(&(buf->forknum), sizeof(ForkNumber), 1, file);

			/* Reset trackers appropriately */
			prev_forknum	= buf->forknum;
			prev_blocknum	= InvalidBlockNumber;
			range_counter	= 0;
		}

		if (prev_blocknum != InvalidBlockNumber
			&& prev_blocknum + range_counter + 1 == buf->blocknum)
		{
			/* We're processing a range of consecutive blocks of the relation. */
			++range_counter;
		}
		else
		{
			/*
			 * We encountered a block that's not in the continuous range of the
			 * previous block. Emit a marker for the previous range, if any, and
			 * then emit a marker for this block.
			 */
			if (range_counter != 0)
			{
				fwrite("N", 1, 1, file);
				fwrite(&range_counter, sizeof(range_counter), 1, file);
			}

			fwrite("b", 1, 1, file);
			fwrite(&(buf->blocknum), sizeof(BlockNumber), 1, file);

			/* Reset trackers appropriately */
			prev_blocknum	= buf->blocknum;
			range_counter	= 0;
		}
	}

	/* The file should be released before transaction end, not after. */
	Assert(file != NULL);
	FreeFile(file);

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

	/*
	ereport(LOG,(errmsg("comparing buffers: (%d, %d, %d, %d) : (%d, %d, %d, %d)",
						a->database, a->filenode, a->forknum, a->blocknum,
						b->database, b->filenode, b->forknum, b->blocknum)));
	*/
	svdbfrcmp(database);
	svdbfrcmp(filenode);
	svdbfrcmp(forknum);
	svdbfrcmp(blocknum);

	/*
	 * TODO: Ask pgsql-hackers why we get all-zero buffers, even though we're locking
	 * and inspecting buffer headers for BM_TAG_VALID, and even BM_VALID, flags.
	 */
	ereport(LOG,(errmsg("Found two identical buffers: (%d, %d, %d, %d)",
						a->database, a->filenode, a->forknum, a->blocknum)));

	Assert(false);	// No two buffers should be storing identical page

	return 0;	// Keep compiler happy.
}

static Oid
GetRelOid(Oid filenode)
{
	StringInfoData buf;
	int			ret;
	Oid			relid;
	bool		isnull;


	initStringInfo(&buf);
	appendStringInfo(&buf, "select oid from pg_class where pg_relation_filenode(oid) = %d",
					 filenode);

	/* TODO: Use SPI_prepare() to prepare a plan, preserved across calls. */
	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed > 1)
		elog(FATAL, "not a singleton result");

	if (SPI_processed < 1)
		return InvalidOid;

	relid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));
	if (isnull)
		return InvalidOid;

	return relid;
}
