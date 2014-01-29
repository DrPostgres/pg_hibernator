#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>


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
static const char* getDatabaseSaveFileName(int number, const char* const dbname);
static void		WorkerCommon(void);
static int		SavedBufferCmp(const void *a, const void *b);
static Oid		GetRelOid(Oid filenode);
static bool		parseSaveFileName(const char *fname, int * number, char *dbname);
static FILE*	fileOpen(const char *path, const char *mode);
static bool		fileClose(FILE *file, const char *path);
static bool		fileRead(void *dest, size_t size, size_t n, FILE *file, bool eof_ok, const char *path);
static bool		fileWrite(const void *src, size_t size, size_t n, FILE *file, const char *path);

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
getDatabaseSaveFileName(int number, const char *dbname)
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
	 * Create the BufferSaver irrespective of whether the extension is enabled. The
	 * BufferSaver will check the flag when it receives SIGTERM, and act accordingly.
	 * This is done so that the user can start the server with the extension disabled,
	 * enable the extension while server is running, and expect the save-files to be
	 * created when the server shuts down.
	 */
	CreateWorker(0);	/* Create the BufferSaver worker */

	/* Don't create BlockReaders if the extension is disabled. */
	if (!hibernate_enabled)
		return;

	hibernate_dir = getSaveDirName();

	dir = opendir(hibernate_dir);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", hibernate_dir)));

	errno = 0;
	while ((dent = readdir(dir)) != NULL)
	{
		int		number;
		char	dbname[NAMEDATALEN];

		if (!parseSaveFileName(dent->d_name, &number, dbname))
			continue;

		CreateWorker(number);
	}

	if (errno != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("encountered error during readdir \"%s\": %m", hibernate_dir)));

	closedir(dir);
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
		worker.bgw_restart_time	= BGW_NEVER_RESTART;	/* Don't restart BlockReaders upon error */
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

	dir = opendir(hibernate_dir);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("Hibernate Block Reader %d: could not open directory \"%s\": %m",
						index, hibernate_dir)));

	errno = 0;
	while ((dent = readdir(dir)) != NULL)
	{
		if (!parseSaveFileName(dent->d_name, &number, dbname))
			continue;

		if (number == index)
			break;
	}

	if (dent == NULL)
	{
		closedir(dir);

		if (errno != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("Hibernate Block Reader %d: encountered error during readdir \"%s\": %m",
							index, hibernate_dir)));

		ereport(ERROR,
				(errmsg("Hibernate Block Reader %d: could not find its save-file", index)));
	}
	closedir(dir);

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
	ereport(LOG, (errmsg("Hibernate Block Reader %d: all blocks read successfully", index)));
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
	Relation	rel;
	bool		skip_relation = false;
	bool		skip_fork = false;
	bool		skip_block = false;
	int64		nblocks = 0;
	const char *filename;

	filename = getDatabaseSaveFileName(number, dbname);
	file = fileOpen(filename, PG_BINARY_R);

	/* To restore the global objects, use default database */
	BackgroundWorkerInitializeConnection(number == 1 ? default_database : dbname, NULL);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "restoring buffers");

	rel = NULL;

	/*
	 * Note that in case of a read error, we will leak relcache entry that we may
	 * currently have open. In case of EOF, we close the relation after the loop.
	 */
	while (fileRead(&record_type, 1, 1, file, true, filename))
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
		 * be to allow the user to interrupt the BlockReader's operation by
		 * changing this extenstion's GUC parameter. But the user can do that
		 * anyway, using SIGTERM or pg_terminate_backend().
		 */

		/* Stop processing the save-file if the Postmaster wants us to die. */
		if (got_sigterm)
			break;

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

				fileRead(&record_filenode, sizeof(Oid), 1, file, false, filename);

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
				nblocks = 0;

				if (skip_relation)
					continue;

				Assert(rel != NULL);
				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a fork record without a preceeding relation record")));

				fileRead(&record_forknum, sizeof(ForkNumber), 1, file, false, filename);

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
				Assert(rel != NULL);
				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a block record without a preceeding relation record")));

				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errmsg("found a block record without a preceeding fork record")));

				fileRead(&record_blocknum, sizeof(BlockNumber), 1, file, false, filename);

				ereport(LOG,
						(errmsg("DB %d reading block filenode %d forknum %d blocknum %d",
								number, record_filenode, record_forknum, record_blocknum)));

				if (skip_relation || skip_fork)
					continue;

				ereport(log_level, (errmsg("processing block %d", record_blocknum)));

				/*
				 * Don't try to read past the file; the file may have been shrunk
				 * by a vaccum operation.
				 */
				if (record_blocknum >= nblocks)
				{
					ereport(log_level,
							(errmsg("skipping filenode %d forknum %d blocknum %d",
									record_filenode, record_forknum, record_blocknum)));

					skip_block = true;
					continue;
				}
				else
				{
					Buffer	buf;

					skip_block = false;

					buf = ReadBufferExtended(rel, record_forknum, record_blocknum, RBM_NORMAL, NULL);
					ReleaseBuffer(buf);
				}
			}
			break;
			case 'N':
			{
				int64 block;

				Assert(record_blocknum != InvalidBlockNumber);
				if (rel == NULL)
					ereport(ERROR,
							(errmsg("found a block range record without a preceeding relation record")));

				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errmsg("found a block range record without a preceeding fork record")));

				if (record_blocknum == InvalidBlockNumber)
					ereport(ERROR,
							(errmsg("found a block range record without a preceeding block record")));

				fileRead(&record_range, sizeof(int), 1, file, false, filename);

				ereport(LOG,
						(errmsg("DB %d reading range filenode %d forknum %d blocknum %d range %d",
								number, record_filenode, record_forknum, record_blocknum, record_range)));

				if (skip_relation || skip_fork || skip_block)
					continue;

				for (block = record_blocknum + 1; block <= (record_blocknum + record_range); ++block)
				{
					Buffer	buf;

					/*
					* Don't try to read past the file; the file may have been shrunk
					* by a vaccum operation.
					*/
					if (block >= nblocks)
						break;

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

	if (rel)
		relation_close(rel, AccessShareLock);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	fileClose(file, filename);

	/* Remove the save-file */
	if (remove(filename) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error removing file \"%s\" : %m", filename)));
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
		 * postmaster dies. This way the background process goes away immediately
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
	int						i;
	int						num_buffers;
	SavedBuffer			   *saved_buffers;
	volatile BufferDesc	   *bufHdr;			// XXX: Do we really need volatile here?
	FILE				   *file			= NULL;
	int						database_number;
	Oid						prev_database	= InvalidOid;
	Oid						prev_filenode	= InvalidOid;
	ForkNumber				prev_forknum	= InvalidForkNumber;
	BlockNumber				prev_blocknum	= InvalidBlockNumber;
	uint					range_counter	= 0;
	const char			   *savefile_name;

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
			saved_buffers[num_buffers].database	= bufHdr->tag.rnode.dbNode;
			saved_buffers[num_buffers].filenode	= bufHdr->tag.rnode.relNode;
			saved_buffers[num_buffers].forknum	= bufHdr->tag.forkNum;
			saved_buffers[num_buffers].blocknum	= bufHdr->tag.blockNum;

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
	for (i = NUM_BUFFER_PARTITIONS - 1; i >= 0; --i)
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

#define WRITE_RANGE_RECORD()		\
	do {							\
			if (range_counter != 0)	\
			{						\
				ereport(LOG,		\
					(errmsg("DB %d writing range filenode %d forknum %d blocknum %d range %d",				\
							database_number, prev_filenode, prev_forknum, prev_blocknum, range_counter)));	\
																							\
				fileWrite("N", 1, 1, file, savefile_name);									\
				fileWrite(&range_counter, sizeof(range_counter), 1, file, savefile_name);	\
			}																				\
	} while(0)

	for (i = 0; i < num_buffers; ++i)
	{
		SavedBuffer *buf = &saved_buffers[i];

		if (buf->database == 0)
		{
			/* Special case for global objects */

			savefile_name = getDatabaseSaveFileName(database_number, "global");
			file = fileOpen(savefile_name, PG_BINARY_W);
		}
		else if (buf->database != prev_database)
		{
			char *dbname;

			WRITE_RANGE_RECORD();
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
			fileClose(file, savefile_name);

			savefile_name = getDatabaseSaveFileName(database_number, dbname);
			file = fileOpen(savefile_name, PG_BINARY_W);

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
			WRITE_RANGE_RECORD();

			/* We're beginning to process a new relation; emit a record for it. */
			fileWrite("r", 1, 1, file, savefile_name);
			fileWrite(&(buf->filenode), sizeof(Oid), 1, file, savefile_name);

			/* Reset trackers appropriately */
			prev_filenode	= buf->filenode;
			prev_forknum	= InvalidForkNumber;
			prev_blocknum	= InvalidBlockNumber;
			range_counter	= 0;
		}

		if (buf->forknum != prev_forknum)
		{
			WRITE_RANGE_RECORD();

			/*
			 * We're beginning to process a new fork of this relation; add a record
			 * for it.
			 */
			fileWrite("f", 1, 1, file, savefile_name);
			fileWrite(&(buf->forknum), sizeof(ForkNumber), 1, file, savefile_name);

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
			 * previous block. Emit a record for the previous range, if any, and
			 * then emit a record for this block.
			 */
			WRITE_RANGE_RECORD();
			
			ereport(LOG,
					(errmsg("DB %d writing block filenode %d forknum %d blocknum %d",
							database_number, prev_filenode, prev_forknum, buf->blocknum)));

			fileWrite("b", 1, 1, file, savefile_name);
			fileWrite(&(buf->blocknum), sizeof(BlockNumber), 1, file, savefile_name);

			/* Reset trackers appropriately */
			prev_blocknum	= buf->blocknum;
			range_counter	= 0;
		}
	}

	/*
	 * We might have exited the above loop while we were still in the middle of
	 * processing a continuous range of blocks. Emit that record now.
	 */
	WRITE_RANGE_RECORD();

	Assert(file != NULL);
	fileClose(file, savefile_name);

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
											SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		return InvalidOid;

	return relid;
}

/* Returns FILE* on success, doesn't return on error */
static FILE*
fileOpen(const char *path, const char *mode)
{
	FILE *file = fopen(path, mode);
	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open \"%s\": %m", path)));

	return file;
}

/* Returns true on success, doesn't return on error */
static bool
fileClose(FILE *file, const char *path)
{
	int rc;

	rc = fclose(file);
	if (rc != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("encountered error %d while closing file \"%s\": %m",
						rc, path)));

	return true;
}

/* Returns true on success, doesn't return on error. */
static bool
fileRead(void *dest, size_t size, size_t n, FILE *file, bool eof_ok, const char *path)
{
	if (fread(dest, size, n, file) != n)
	{
		if (feof(file))
		{
			if (eof_ok)
				return false;
			else
				ereport(ERROR,
						(errmsg("found EOF when not expecting one \"%s\"", path)));
		}
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("error reading \"%s\" : %m", path)));
	}

	return true;
}

/* Returns true on success, doesn't return on error. */
static bool
fileWrite(const void *src, size_t size, size_t n, FILE *file, const char *path)
{
	if (fwrite(src, size, n, file) != n)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error writing to \"%s\" : %m", path)));
	}

	return true;
}
