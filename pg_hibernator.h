
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

/* Header files needed by this extension */
#include "access/xact.h"
#include "catalog/pg_type.h"
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
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"

/* Functions defined in misc.c */
extern bool		parseSavefileName(const char *fname, int *filenum);
extern FILE*	fileOpen(const char *path, const char *mode);
extern bool		fileClose(FILE *file, const char *path);
extern bool		fileRead(void *dest, size_t size, FILE *file, bool eof_ok, const char *path);
extern bool		fileWrite(const void *src, size_t size, FILE *file, const char *path);
extern bool		writeDBName(const char *dbname, FILE *file, const char *path);
extern char*	readDBName(FILE *file, const char *path);
extern const char* getSavefileName(int filenum);

/* Constants */
#define SAVE_LOCATION "pg_hibernator"

