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

typedef struct SavedBuffer
{
	Oid			database;
	Oid			filenode;	/* Stream marker: 'r' */
	ForkNumber	forknum;	/* Stream marker: 'f' */
	BlockNumber	blocknum;	/* Stream marker: 'b' */
							/* Stream marker: 'N' for range of blocks */
} SavedBuffer;

/* Forward declarations */
static void		DefineGUCs(void);
static void		CreateDirectory(void);
static List*	ReadDirectory(void);
static List*	ReadSavedFile(unsigned int i, char **dbname);
static void		RestoreRelation(SavedBuffer *r);
static void		SaveBuffers(void);
static void		CreateWorkers(void);
static void		CreateWorker(int id);
static char*	getSaveDirName();
static char*	getDatabseSaveFileName(int database_number, char *dbname);
static void		WorkerCommon(void);
static void		WorkerMain(Datum main_arg);
static void		BufferSaverMain(Datum main_arg);
static int		SavedBufferCmp(const void *a, const void *b);
static Oid		GetRelOid(Oid filenode);

PG_MODULE_MAGIC;

void	_PG_init(void);

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

	/* TODO: Remove this GUC; doesn't seem to be useful, as of now. */
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

#define SAVE_LOCATION "pg_hibernate"

static char*
getSaveDirName()
{
	static char	hibernate_dir[sizeof(SAVE_LOCATION) + 1];

	if (hibernate_dir[0] == '\0')
		snprintf(hibernate_dir, sizeof(hibernate_dir), "%s", SAVE_LOCATION);

	return hibernate_dir;
}

static char*
getDatabseSaveFileName(int database_number, char *dbname)
{
	char *ret = palloc(MAXPGPATH);

	snprintf(ret, MAXPGPATH, "%s/%d.%s.save", getSaveDirName(), database_number, dbname);

	return ret;
}

static void
CreateDirectory(void)
{
	struct stat	st;
	char	   *hibernate_dir = getSaveDirName();

	if (stat(hibernate_dir, &st) == 0)
	{
		/* Is it a directory? */
		if (!S_ISDIR(st.st_mode))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" exists but is not a directory, hence disabling hibernation",
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
	DIR				   *dir;
	char			   *hibernate_dir = getSaveDirName();
	struct dirent	   *dent;

	CreateWorker(0);	/* Create the buffer-saver worker */

    dir = AllocateDir(hibernate_dir);
    if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", hibernate_dir)));

    while ((dent = ReadDir(dir, hibernate_dir)) != NULL)
    {
		int		number;
		char	dbname[NAMEDATALEN];

		if (sscanf(dent->d_name, "%d.%s.save", &number, dbname) != 2)
			continue;	/* Skip if the name doesn't match the format we're expecting */

		CreateWorker(number);
    }

	FreeDir(dir);
}

static void
CreateWorker(int id)
{
	BackgroundWorker	worker;

	/* TODO: See if we can avoid the database-connection bit */
	worker.bgw_flags		= BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time	= BgWorkerStart_ConsistentState;
	worker.bgw_restart_time	= id == 0 ? 0 : BGW_NEVER_RESTART;
	worker.bgw_main			= id == 0 ? BufferSaverMain : WorkerMain;

	snprintf(worker.bgw_name, BGW_MAXLEN, id == 0 ? "Hibernate Buffer Saver" : "Hibernate Restorer %d", id);
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
WorkerMain(Datum main_arg)
{
	int					index = DatumGetInt32(main_arg);
	DIR				   *dir;
	char			   *hibernate_dir = getSaveDirName();
	FILE			   *file;
	struct dirent	   *dent;
	int					number;
	char				dbname[NAMEDATALEN];

	char				record_type;
	Oid					record_filenode;
	ForkNumber			record_forknum;
	BlockNumber			record_blocknum;
	int					record_range;
	
	Oid					relOid;
	Relation				rel = NULL;
	bool				skip_relation = false;
	bool				skip_fork = false;
	bool				skip_block = false;
	int64				nblocks = 0;
	char				blockbuffer[BLCKSZ];

	WorkerCommon();

	dir = AllocateDir(hibernate_dir);
    if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("hibernate worker %d could not open directory \"%s\": %m",
						index, hibernate_dir)));

    while ((dent = ReadDir(dir, hibernate_dir)) != NULL)
    {
		int dbname_len;

		if (sscanf(dent->d_name, "%d.%s", &number, dbname) != 2)
			continue;	/* Skip if the name doesn't match the format we're expecting */

		dbname_len = strlen(dbname);
		
		if (strcmp(&dbname[dbname_len - 5], ".save") != 0)
			continue;	/* Skip if the name doesn't match the format we're expecting */
		
		dbname[dbname_len - 5] = '\0';

		if (number == index)
			break;
    }

    if (dent == NULL)
	{
		FreeDir(dir);
		ereport(ERROR,
				(errmsg("hibernate worker %d could not find its save-file", index)));
	}
	FreeDir(dir);
	
	/* TODO: Honor SIGTERM and SIGHUP signals in this worker, too. */

	/* We found the file we're supposed to restore. */

	//{bool stop = true; while(stop)pg_usleep(1000*1000);}

	/* TODO: getDatabseSaveFileName() call leaks memory. */
	file = fopen(getDatabseSaveFileName(number, dbname), PG_BINARY_R);
	if (file == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("could not open save-file for %s", dbname)));

	BackgroundWorkerInitializeConnection(number == 1 ? default_database : dbname, NULL);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "restoring buffers");

	while (!ferror(file))
	{
		if (fread(&record_type, 1, 1, file) != 1)
		{
			/* Most-likely end-of-file */
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
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("error reading save-file of %s", dbname)));
				}
				
				relOid = GetRelOid(record_filenode);

				ereport(DEBUG3, (errmsg("processing filenode %d, relation %d",
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
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("found a fork marker without a preceeding relation marker")));

				if (fread(&record_forknum, sizeof(ForkNumber), 1, file) != 1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("error reading save-file of %s", dbname)));
				}

				ereport(LOG, (errmsg("processing fork %d", record_forknum)));
				
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
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("found a block marker without a preceeding relation marker")));
				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("found a block marker without a preceeding fork marker")));

				if (fread(&record_blocknum, sizeof(BlockNumber), 1, file) != 1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("error reading save-file of %s", dbname)));
				}
				ereport(LOG, (errmsg("processing block %d", record_blocknum)));

				/*
				 * Don't try to read past the file; the file may have been shrunk
				 * by a vaccum operation.
				 */
				if (record_blocknum > nblocks)
				{
					skip_block = true;
					continue;
				}
				else
				{
					skip_block = false;
					smgrread(rel->rd_smgr, record_forknum, record_blocknum, blockbuffer);
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
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("found a block range marker without a preceeding relation marker")));
				if (record_forknum == InvalidForkNumber)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("found a block range marker without a preceeding fork marker")));
				if (record_blocknum == InvalidBlockNumber)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("found a block range marker without a preceeding block marker")));

				if (fread(&record_range, sizeof(int), 1, file) != 1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("error reading save-file of %s", dbname)));
				}
				ereport(LOG, (errmsg("processing range %d", record_range)));

				for (block = record_blocknum; block < (record_blocknum + record_range); ++block)
				{
					smgrread(rel->rd_smgr, record_forknum, block, blockbuffer);
				}
			}
			break;
			default:
			{
				ereport(LOG, (errmsg("found unexpected save-file marker %x - %c", record_type, record_type)));
				continue;

				Assert(false);
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("found unexpected save-file marker %x (%c)", record_type, record_type)));
			}
			break;
		}
	}
	if (ferror(file))
		ereport(ERROR,
				(errmsg("error reading save-file of %s : %m", dbname)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	fclose(file);

	/* Exit with non-zero status to ensure that this worker is not restarted */
	proc_exit(1);
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

	/* Scan an save a list of valid buffers. */
	for (num_buffers = 0, i = 0, bufHdr = BufferDescriptors; i < NBuffers; ++i, ++bufHdr)
	{
		/* Lock each buffer header before inspecting. */
		LockBufHdr(bufHdr);

		/* Skip invalid buffers */
		if ( ((bufHdr->flags & BM_VALID) && (bufHdr->flags & BM_TAG_VALID)))
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
	 * just as we are. Also, it doensn't hurt to follow the protocol.
	 */
	for (i = NUM_BUFFER_PARTITIONS; --i >= 0;)
		LWLockRelease(FirstBufMappingLock + i);

	/* Sort the buffers, so that we can optimize the storage of these buffers. */
	pg_qsort(saved_buffers, num_buffers, sizeof(SavedBuffer), SavedBufferCmp);

	/*
	 * Number 0 is reserved; In CreateWorkers(), 0 is used to identify and register
	 * the BufferSaver.
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

			/* TODO: getDatabseSaveFileName() call leaks memory */
			file = AllocateFile(getDatabseSaveFileName(database_number, "global"), PG_BINARY_W);

			if (file == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("could not create save-file for global objects")));
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
			/* TODO: getDatabseSaveFileName() call leaks memory */
			file = AllocateFile(getDatabseSaveFileName(database_number, dbname), PG_BINARY_W);

			if (file == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("could not create save-file for %s database", dbname)));

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
			/*
			 * We're beginning to process a new relation; add a marker for this
			 * relation.
			 */

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
			/* We're beginning to process a new fork of this relation; add a marker
			 * for this fork.
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

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	relid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));
	if (isnull)
		return InvalidOid;

	return relid;
}
