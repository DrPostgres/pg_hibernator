# Postgres Hibernator

This Postgres extension is a set-it-and-forget-it solution to save and restore
the Postgres shared-buffers contents, across Postgres server restarts.

It performs the automatic save and restore of database buffers, integrated with
database shutdown and startup, hence reducing the durations of database maintenance
windows, in effect increasing the uptime of your applications.

Postgres Hibernator automatically saves the list of shared buffers to the disk
on database shutdown, and automatically restores the buffers on database startup.
This acts pretty much like your Operating System's hibernate feature, except,
instead of saving the contents of the memory to disk, Postgres Hibernator saves
just a list of block identifiers. And it uses that list after startup to restore
the blocks from data directory into Postgres' shared buffers.

## Unique Feature

To my knowledge, **no** other RDBMS/DBMS, commercial or open-source, provides
this capability of saving and restoring database cache across server restarts.

## Why

DBAs are often faced with the task of performing some maintenance on their
database server(s) which requires shutting down the database. The maintenance
may involve anything from a database patch application, to a hardware upgrade.
One ugly side-effect of restarting the database server/service is that all the
data currently in database server's memory will be all lost, which was
painstakingly fetched from disk and put there in response to application queries
over time. And this data will have to be rebuilt as applications start querying
database again. The query response times will be very high until all the “hot”
data is fetched from disk and put back in memory again.

People employ a few tricks to get around this ugly truth, which range from
running a `select * from app_table;`, to `dd if=table_file ...`, to using
specialized utilities like pgfincore to prefetch data files into OS cache.
Wouldn't it be ideal if the database itself could save and restore its memory
contents across restarts!

The duration for which the server is building up caches, and trying to reach its
optimal cache performance is called ramp-up time. Postgres Hibernator is aimed
at reducing the ramp-up time of Postgres servers.

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
block to the disk. This information is saved under the `$PGDATA/pg_hibernator/`
directory. For each of the database whose blocks are resident in shared buffers,
one file is created; for eg.: `$PGDATA/pg_hibernator/2.save`.

During the next startup sequence, the `Block Reader` threads are registerd, one for
each file present under `$PGDATA/pg_hibernator/` directory. When the Postgres server
has reached stable state (that is, it's ready for database connections), these
`Block Reader` processes are launched. The `Block Reader` process reads the save-files
looking for block-ids to restore. It then connects to the respective database,
and requests Postgres to fetch the blocks into shared-buffers.

## Configuration

This extension can be controlled via the following parameters. These parameters
can be set in postgresql.conf or on postmaster's command-line.

- `pg_hibernator.enabled`

    Setting this parameter to false disables the hibernator features. That is,
    on server startup the BlockReader processes will not be launched, and on
    server shutdown the list of blicks in shared buffers will not be saved.

    Note that the BuffersSaver process exists at all times, even when this
    parameter is set to `false`. This is to allow the DBA to enable/disable the
    extension without having to restart the server. The BufferSaver process
    checks this parameter during server startup and right before shutdown, and
    honors this parameter's value at that time.

    To enable/disable Postgres Hibernator at runtime, change the value in
    `postgresql.conf` and use `pg_ctl reload` to make Postgres re-read the new
    parameter values from `postgresql.conf`.

    Default value: `true`.

- `pg_hibernator.parallel`

    This parameter controls whether Postgres Hibernator launches the BlockReader
    processes in parallel, or sequentially, waiting for current BlockReader to
    exit before launching the next one.

    When enabled, all the BlockReaders, one for each database, will be launched
    simultaneously, and this may cause huge random-read flood on disks if there
    are many databases in cluster. This may also cause some BlockReaders to fail
    to launch successfully because of `max_worker_processes` limit.

    Default value: `false`.

- `pg_hibernator.default_database`

    The BufferSaver process needs to connect to a database in order to perform
    the database-name lookups etc. This parameter controls which database the
    BufferSaver process connects to for perfoming these operations.

    Default value: `postgres`.

## Caveats

- Buffer list is saved only when Postgres is shutdown in "smart" and "fast" modes.

    That is, buffer list is not saved when database crashes, or on "immediate"
    shutdown.

- A reduction in `shared_buffers` is not detected.

    If the `shared_buffers` is reduced across a restart, and if the combined
    saved buffer list is larger than the new shared_buffers lize, Postgres
    Hibernator continues to read and restore blocks even after `shared_buffers`
    worth of buffers have been restored.

## Nice-to-have features

- Save/restore the filesystem buffers or disk cache

    It's very desirable to save/restore filesystem buffers, that back the data
    directory, across database server restart. This will restore those data
    blocks to memory that were read/modified by Postgres, but then evicted from
    shared buffers, and yet are available in filesystem/disk cache.

    I looked at `pgfincore`, but decided against including that feature-set
    in Postgres Hibernator because, (a) `pgfincore` requires the user to know
    beforehand which tables/indexes they will need in future, and (b) Postgres
    Hibernator is supposed to be as invisible to the user/DBA as much as possible.

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
        startup, load those blocks back into shared buffers.

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

    [Introducing Postgres Hibernator](http://gurjeet.singh.im/blog/2014/02/03/introducing-postgres-hibernator/)

    [ Demonstrating Performance Benefits](http://gurjeet.singh.im/blog/2014/04/30/postgres-hibernator-reduce-planned-database-down-times/)

[postgres_site]: http://www.postgresql.org
[ppas_site]: http://enterprisedb.com/products-services-training/products/postgres-plus-advanced-server
[gurjeet_site]: http://gurjeet.singh.im
