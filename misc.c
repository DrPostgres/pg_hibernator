
#include "postgres.h"
#include "pg_hibernator.h"

/* Returns FILE* on success, doesn't return on error */
FILE*
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
bool
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
bool
fileRead(void *dest, size_t size, FILE *file, bool eof_ok, const char *path)
{
	if (fread(dest, size, 1, file) != 1)
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
bool
fileWrite(const void *src, size_t size, FILE *file, const char *path)
{
	if (fwrite(src, size, 1, file) != 1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("error writing to \"%s\" : %m", path)));

	return true;
}

/*
 * We use static array here, because the returned pointer is not modified by
 * the callers, and they call this function everytime they need new value. This
 * allows us to avoid the hassle of allocating and free'ing memory.
 */
char *
readDBName(FILE *file, const char *path)
{
	static char dbname[NAMEDATALEN];
	char ch;
	int char_count;

	for(char_count = 0; char_count <= NAMEDATALEN; ++char_count)
	{
		fileRead(&ch, 1, file, false, path);

		dbname[char_count] = ch;

		if (ch == '\0')
			break;
	}

	if (ch != '\0')
		ereport(ERROR,
				(errmsg("error reading database name from \"%s\"", path)));

	return dbname;
}

bool
writeDBName(const char *dbname, FILE *file, const char *path)
{
	/* Include the null terminator */
	return fileWrite(dbname, strlen(dbname)+1, file, path);
}

/*
 * We use static array here, because the returned pointer is not modified by
 * the callers, and they call this function everytime they need new value. This
 * allows us to avoid the hassle of allocating and free'ing memory.
 */
const char*
getSavefileName(int filenum)
{
	static char ret[MAXPGPATH];

	snprintf(ret, sizeof(ret), "%s/%d.save", SAVE_LOCATION, filenum);

	return ret;
}

bool
parseSavefileName(const char *fname, int *filenum)
{
	/*
	 * Enough storage to contain '.save' followed by null character. We add one
	 * more byte to catch spurious characters past the valid suffix, if any.
	 */
	char suffix[7];
	char *valid_suffix = ".save";

	/* The save-file name format is: <integer>.save */

	if (sscanf(fname, "%d%6s", filenum, suffix) != 2)
		return false;	/* Fail if the name doesn't contain an integer followed by a string */

	if (0 != strncmp(valid_suffix, suffix, strlen(valid_suffix)))
		return false;	/* Fail if the suffix isn't what we expect it to. */

	return true;
}
