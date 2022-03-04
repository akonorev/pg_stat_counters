EXTENSION    = pg_stat_counters
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test


PG_CONFIG = pg_config

MODULE_big = $(EXTENSION)
OBJS = $(EXTENSION).o

all: 

release-zip: all
	git archive --format zip --prefix=$(EXTENSION)-${EXTVERSION}/ --output ./$(EXTENSION)-${EXTVERSION}.zip HEAD
	unzip ./$(EXTENSION)-$(EXTVERSION).zip
	rm ./$(EXTENSION)-$(EXTVERSION).zip
	rm ./$(EXTENSION)-$(EXTVERSION)/.gitignore
	zip -r ./$(EXTENSION)-$(EXTVERSION).zip ./$(EXTENSION)-$(EXTVERSION)/
	rm ./$(EXTENSION)-$(EXTVERSION) -rf

DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


