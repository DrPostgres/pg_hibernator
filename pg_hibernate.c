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

/* Includes needed for this plugin */
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"

typedef struct SavedRelation
{
	Oid			filenode;
	ForkNumber	forknum;
	BlockNumber	block;

}SavedRelation;

/* Forward declarations */
static void		DefineGUCs(void);
static void		CreateDirectory(void);
static List*	ReadDirectory(void);
static List*	ReadSavedFile(unsigned int i, char **dbname);
static void		RestoreRelation(SavedRelation *r); // RelidByRelfilenode()
static void		SaveBuffers(void);
static void		CreateWorkers(void);
static void		CreateWorker(int id);
static char*	getSaveDirName();
static void		WorkerMain(Datum main_arg);
static void		BufferSaverMain(Datum main_arg);

PG_MODULE_MAGIC;

void	_PG_init(void);

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

#define SAVE_LOCATION "hibernate"

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

static char*
getSaveDirName()
{
	static char	hibernate_dir[MAXPGPATH];
	int			nbytes = strlen(SAVE_LOCATION) + 1;

	if (hibernate_dir[0] != '\0')
		snprintf(hibernate_dir, nbytes, "%s", SAVE_LOCATION);

	return hibernate_dir;
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
					 errmsg("\"%s\" exists but is not a directory. Disabling hibernation.",
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

	snprintf(worker.bgw_name, BGW_MAXLEN, "Hibernate Restorer %d", id);
	worker.bgw_main_arg = Int32GetDatum(id);

	RegisterBackgroundWorker(&worker);
}

static void
WorkerMain(Datum main_arg)
{
	int					index = DatumGetInt32(main_arg);
	DIR				   *dir;
	char			   *hibernate_dir = getSaveDirName();
	struct dirent	   *dent;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, sighupHandler);
	pqsignal(SIGTERM, sigtermHandler);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	dir = AllocateDir(hibernate_dir);
    if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("hibernate worker %d could not open directory \"%s\": %m",
						index, hibernate_dir)));

    while ((dent = ReadDir(dir, hibernate_dir)) != NULL)
    {
		int		number;
		char	dbname[NAMEDATALEN];

		if (sscanf(dent->d_name, "%d.%s.save", &number, dbname) != 2)
			continue;	/* Skip if the name doesn't match the format we're expecting */

		if (number == index)
			break;
    }

    if (dent == NULL)
		ereport(ERROR,
				(errmsg("hibernate worker %d could not find its save file", index)));

	/* Honor SIGTERM and SIGHUP signals in this worker, too. */
	/* We found the file we're supposed to restore. */
	FreeDir(dir);
}

static void
BufferSaverMain(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, sighupHandler);
	pqsignal(SIGTERM, sigtermHandler);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

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
}
