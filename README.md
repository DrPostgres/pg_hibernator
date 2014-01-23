# Design

Register a background worker, to be started after Postgres has reached consistent
state, with the ability to connect to the shared buffers and be able to connect and
query a database.

During startup:

- Create `$PGDATA/pg_hibernate/` directory, if it doesn't exists already.
- Read the list of files in that directory. The filenames are formatted as `<db_name>.save`
- For each such file read, register a new background worker, with the `db_name` as the parameter.

After Statup: wait for shutdown.

During Shutdown:

- Scan the shared buffers and extract the list of <relation:page> pairs.
- Save this list to `$PGDATA/pg_hibernate/`, one file per database.

In the background workers that are spawned by the first background worker, read the
file that matches the parameter passed to it, and load the pages listed there into
shared buffers.
