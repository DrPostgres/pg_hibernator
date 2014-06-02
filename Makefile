# contrib/pg_hibernator/Makefile

MODULE_big = pg_hibernator
OBJS = pg_hibernate.o pg_hibernate_9.3.o misc.o

PG_CONFIG = pg_config

# Get the version string from pg_config
VERSION := $(shell $(PG_CONFIG) --version | awk '{print $$2}')
ifeq ("$(VERSION)","")
$(error pg_config not found)
endif

# Extract major version and convert to integer, e.g. 9.1.4 -> 901
INTVERSION := $(shell echo $$(($$(echo $(VERSION) | sed 's/\([[:digit:]]\{1,\}\)\.\([[:digit:]]\{1,\}\).*/\1*100+\2/'))))

# We support PostgreSQL 9.3 and later.
ifeq ($(shell echo $$(($(INTVERSION) < 903))),1)
$(error pg_hibernator requires PostgreSQL 9.3 or later. This is $(VERSION))
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
