CC=/usr/bin/gcc
LDFLAGS=-lcurl -lz

all:
	${CC} -g -o transfer-logs \
	transfer-logs.c junzip.c ezxml.c ${LDFLAGS}
 
