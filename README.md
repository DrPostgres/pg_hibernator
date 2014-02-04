# Postgres Hibernator

This Postgres extension is a set-it-and-forget-it solution to save and restore
the Postgres shared-buffers contents, across Postgres server restarts.

For some details on the internals of this extension, also see the [proposal]
email to Postgres hackers' mailing list.

[proposal]: http://www.postgresql.org/message-id/CABwTF4Ui_anAG+ybseFunAH5Z6DE9aw2NPdy4HryK+M5OdXCCA@mail.gmail.com

## Why

When a database server is shut down, for any reason (say, to apply patches, for
scheduled maintenance, etc.), the active data-set that is cached in memory by
the database server is lost. Upon starting up the server again, the database
server's cache is empty, and hence almost all application queries respond slowly
because the server has to fetch the relevant data from the disk. It takes quite a
while for the server to bring the cache back to similar state as before the server
shutdown.

The duration for which the server is building up caches, and trying to reach its
optimal cache performance is called ramp-up time.

This extension is aimed at reducing the ramp-up time of Postgres servers.

## How

Compile and install the extension (of course, you'd need Postgres installation or
source code):

    $ make -C pg_hibernate/ install

Then.

1. Add `pg_hibernate` to the `shared_preload_libraries` variable in `postgresql.conf` file.
2. Restart the Postgres server.
3. You are done.

## How it works

This extension uses the `Background Worker` infrastructure of Postgres, which was
introduced in Postgres 9.3. When the server starts, this extension registers
background workers; one for saving the buffers (called `Buffer Saver`) when the
server shuts down, and one for each database in the cluster (called `Block Readers`)
for restoring the buffers saved during previous shutdown.

When the Postgres server is being stopped/shut down, the `Buffer Saver` scans the
shared-buffers of Postgres, and stores the unique block identifiers of each cached
block to the disk (with some optimizatins). This information is saved under the
`$PGDATA/pg_hibernate/` directory. For each of the database whose blocks are
resident in shared buffers, one file is created; for eg.:
`$PGDATA/pg_hibernate/2.postgres.save`.

During the next startup sequence, the `Block Reader` threads are registerd, one for
each file present under `$PGDATA/pg_hibernate/` directory. When the Postgres server
has reached stable state (that is, it's ready for database connections), these
`Block Reader` processes are launched. The `Block Reader` process reads the save-files
looking for block-ids to restore. It then connects to the respective database,
and requests Postgres to fetch the blocks into shared-buffers.

## Caveats
- It saves the buffer information only when Postgres server is shutdown in normal mode.
- It doesn't save/restore the filesystem/kernel's disk cache.

