# Postgres Hibernator

This Postgres extension is a set-it-and-forget-it solution to save and restore
the Postgres shared-buffers contents, across Postgres server restarts.

For some details on the internals of this extension, also see the [proposal]
email to Postgres hackers' mailing list.

[proposal]: http://www.postgresql.org/message-id/CABwTF4Ui_anAG+ybseFunAH5Z6DE9aw2NPdy4HryK+M5OdXCCA@mail.gmail.com

## Unique Feature

To my knowledge, **no** other RDBMS/DBMS, commercial or open-source, provides
this capability of saving and restoring database cache across server restarts.

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

Get the source code of this extension.

    $ wget -O pg_hibernator.zip https://github.com/gurjeet/pg_hibernator/archive/master.zip
    $ unzip -j -d pg_hibernator pg_hibernator.zip

or

    $ git clone https://github.com/gurjeet/pg_hibernator.git

Compile and install the extension (you'll need a Postgres instalation and its
`pg_config` in `$PATH`):

    $ cd pg_hibernator
    $ make install

Then.

1. Add `pg_hibernator` to the `shared_preload_libraries` variable in `postgresql.conf` file.
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
block to the disk (with some optimizations). This information is saved under the
`$PGDATA/pg_hibernator/` directory. For each of the database whose blocks are
resident in shared buffers, one file is created; for eg.:
`$PGDATA/pg_hibernator/2.postgres.save`.

During the next startup sequence, the `Block Reader` threads are registerd, one for
each file present under `$PGDATA/pg_hibernator/` directory. When the Postgres server
has reached stable state (that is, it's ready for database connections), these
`Block Reader` processes are launched. The `Block Reader` process reads the save-files
looking for block-ids to restore. It then connects to the respective database,
and requests Postgres to fetch the blocks into shared-buffers.

## Caveats

- Buffer list is saved only when Postgres is shutdown in "smart" and "fast" modes.

    That is, buffer list is not saved when database crashes, or on "immediate" shutdown.

- A reduction in `shared_buffers` is not detected.

    If the `shared_buffers` is reduced across a restart, Postgres Hibernator
continues to read an restore blocks even after `shared_buffers` worth of buffers
have been restored.

- All Block Readers (one for each DB in cluster) are launched at the same time.

    This may cause huge random-read flood on disks if there are many databases
    in cluster.

    This may also cause some Readers to fail to launch in PG 9.4, because of
    `max_worker_processes`.

## Nice-to-have features

- Save/restore the filesystem buffers or disk cache

    It's very desirable to save/restore filesystem buffers, that back the data
    directory, across database server restart. This will restore those data
    blocks to memory that were read/modified by Postgres, but then evicted from
    shared buffers, and yet are available in filesystem/disk cache.

    I looked at `pgfincore`, but decided against including that feature-set
    in Postgres Hibernator because, (a) `pgfincore` requires the user to know
    beforehand which tables/indexes they will need in future, and
    (b) Postgres Hibernator is supposed to be as invisible to the user/DBA as
    possible.

    One possible solution is to hook into Postgres' buffer-eviction code (at the
    end of `BufferAlloc()`), and keep track of which buffers are being evicted.
    The downsides of this approach are that (a) it adds a huge overhead to an
    otherwise free operation, and (b) the user now needs to be aware of the size
    of filesystem cache, and what portion of it _may_ be dedicated to Postgres'
    data files; hence losing the invisibility that Postgres Hibernator seeks.

- Save/restore a snapshot of buffers

    It may be desirable to have the feature where one can save a snapshot of
    shared buffers, and restore it at a later point.

    I envision it to be useful in cases where a standby has just been created,
    and DBA wants this new standby to have the same buffers contents as the
    master, so that the standby can start serving queries at full speed, as soon
    as possible.

    (Based on a question posted on my blog post)

## FAQ

- What is the relationship between `pg_buffercache`, `pg_prewarm`, and `pg_hibernator`?

    They all allow you to do different things with Postgres' shared buffers.

    + pg_buffercahce:

        Inspect and show contents of shared buffers

    + pg_prewarm:

        Load some table/index/fork blocks into shared buffers. User needs
        to tell it which blocks to load.

    + pg_hibernator:

        Upon shutdown, save list of blocks stored in shared buffers. Upon
        startup, loads those blocks back into shared buffers.

    The goal of Postgres Hibernator is to be invisible to the user/DBA.
    Whereas with `pg_prewarm` the user needs to know a lot of stuff about
    what they really want to do, most likely information gathered via
    `pg_buffercahce`.

- Does `pg_hibernate` use either `pg_buffercache` or `pg_prewarm`?

    No, Postgres Hibernator works all on its own.

    If the concern is, "Do I have to install pg_buffercache and pg_prewarm
    to use pg_hibernator", the answer is no. pg_hibernator is a stand-alone
    extension, although influenced by pg_buffercache and pg_prewarm.

    With `pg_prewarm` you can load blocks of **only** the database you're connected
    to. So if you have `N` databases in your cluster, to restore blocks of all
    databases, the DBA will have to connect to each database and invoke
    `pg_prewarm` functions.

    With `pg_hibernator`, DBA isn't required to do anything, let alone
    connecting to the database!

- Is this an [EDB](http://www.enterprisedb.com) product?

    It's an open-source project, developed by [Gurjeet Singh][gurjeet_site], an
    EDB employee, and contributed to community. It works with both
    [Postgres][postgres_site] and [EDB](http://www.enterprisedb.com).

- What versions of Postgres/EDB are supported.

    Postgres Hibernator supports [Postgres][postgres_site] and EDB's
    [Postgres Plus Advanced Server][ppas_site] products, and versions 9.3 and
    9.4 of both products.

- Where can I learn more about it?

    There are a couple of blog posts and initial proposal to Postgres
    hackers' mailing list. They may provide a better understanding of
    Postgres Hibernator.

    [Proposal](http://www.postgresql.org/message-id/CABwTF4Ui_anAG+ybseFunAH5Z6DE9aw2NPdy4HryK+M5OdXCCA@mail.gmail.com)

    [Introducing Postrges Hibernator](http://gurjeet.singh.im/blog/2014/02/03/introducing-postgres-hibernator/)

    [Demostrating Performance Benefits](http://gurjeet.singh.im/blog/2014/04/30/postgres-hibernator-reduce-planned-database-down-times/)

[postgres_site]: http://www.postgresql.org
[ppas_site]: http://enterprisedb.com/products-services-training/products/postgres-plus-advanced-server
[gurjeet_site]: http://gurjeet.singh.im
