MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src
AM_CPPFLAGS += -I $(top_srcdir)/contrib/zookeeper-3.4.6/include
AM_CPPFLAGS += -I $(top_srcdir)/contrib/json-c-0.12.99/include

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libproto.a

noinst_HEADERS = nc_proto.h

libproto_a_SOURCES =			\
	nc_memcache.c			\
	nc_ssdb.c			\
	nc_redis.c
