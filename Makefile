# contrib/pg_hibernate/Makefile

MODULES = pg_hibernate

PG_CONFIG = pg_config

# Pull out the version number from pg_config
VERSION := $(shell $(PG_CONFIG) --version | awk '{print $$2}')
ifeq ("$(VERSION)","")
$(error pg_config not found)
endif

# version as a number, e.g. 9.1.4 -> 901
INTVERSION := $(shell echo $$(($$(echo $(VERSION) | sed 's/\([[:digit:]]\{1,\}\)\.\([[:digit:]]\{1,\}\).*/\1*100+\2/'))))

# We support PostgreSQL 9.3 and later.
ifeq ($(shell echo $$(($(INTVERSION) < 903))),1)
$(error pg_hibernate requires PostgreSQL 9.3 or later. This is $(VERSION))
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
