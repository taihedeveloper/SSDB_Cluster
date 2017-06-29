MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src
AM_CPPFLAGS += -I $(top_srcdir)/contrib/zookeeper-3.4.6/include
AM_CPPFLAGS += -I $(top_srcdir)/contrib/json-c-0.12.99/include

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libhashkit.a

noinst_HEADERS = nc_hashkit.h

libhashkit_a_SOURCES =		\
	nc_crc16.c		\
	nc_crc32.c		\
	nc_fnv.c		\
	nc_hsieh.c		\
	nc_jenkins.c		\
	nc_ketama.c		\
	nc_md5.c		\
	nc_modula.c		\
	nc_hashslot.c		\
	nc_murmur.c		\
	nc_one_at_a_time.c	\
	nc_random.c
