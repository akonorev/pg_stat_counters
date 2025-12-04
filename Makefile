EXTENSION     = pg_stat_counters
EXTVERSION    = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
GITCOMMIT     = $(shell git describe --dirty --always)
#CITFULLCOMMIT = $(shell git rev-list --tags --max-count=1)
#GITTAG        = $(shell git describe --tags $(GITFULLCOMMIT))
TESTS         = $(wildcard test/sql/*.sql)
REGRESS       = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS  = --inputdir=test

PG_CPPFLAGS = -D_EXTFULLVERSION=\"$(EXTVERSION)-$(GITCOMMIT)\"
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
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

