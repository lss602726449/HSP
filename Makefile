EXTENSION = hsp

OBJS = $(patsubst %.c, %.o, $(wildcard *.c))

MODULE_big = $(EXTENSION)

DATA = hsp--1.0.sql
REGRESS = uint4_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS) 

