BUILD_DIR = build
INCLUDE_DIR = ../include
INCLUDE_PG_DIR = /usr/include/postgresql
LIB_DIR = ../lib

CC=gcc
LD=gcc
CFLAGS=-I$(INCLUDE_DIR) -I$(INCLUDE_PG_DIR) -O2 -g -Wall
LIBS=-L$(LIB_DIR) -lodpic -lpq  -ljansson -lcurl
COMMON_OBJS = $(BUILD_DIR)/request-migrator.o $(BUILD_DIR)/json-migrator.o

SOURCES = data-migrator.c
BINARIES = $(SOURCES:%.c=$(BUILD_DIR)/%)

all: $(BUILD_DIR) $(BINARIES)

clean:
	rm -rf $(BUILD_DIR)

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/%.o: %.c  json-migrator.h request-migrator.h
	$(CC) -c $(CFLAGS) -o $@ $<


$(BUILD_DIR)/%: $(BUILD_DIR)/%.o $(COMMON_OBJS)
	$(LD) $(LDFLAGS) $< -o $@ $(COMMON_OBJS) $(LIBS)
