MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src
AM_CPPFLAGS += -I $(top_srcdir)/contrib/zookeeper-3.4.6/include
AM_CPPFLAGS += -I $(top_srcdir)/contrib/json-c-0.12.99/include

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

sbin_PROGRAMS = migrate

migrate_SOURCES =         \
	../nc_zookeeper.c ../nc_zookeeper.h \
	../nc_log.c ../nc_log.h      \
	../nc_string.c ../nc_string.h \
	../nc_util.c ../nc_util.h  \
	migrate.cpp

migrate_LDADD=./libssdb-client.a
migrate_LDADD+= $(top_builddir)/contrib/zookeeper-3.4.6/.libs/libzookeeper_mt.a
migrate_LDADD+= $(top_builddir)/contrib/zookeeper-3.4.6/.libs/libzookeeper_st.a
migrate_LDADD += $(top_builddir)/contrib/json-c-0.12.99/.libs/libjson-c.a

