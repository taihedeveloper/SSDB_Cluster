/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_INCLUDE_H_
#define SSDB_INCLUDE_H_

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "util/slot.h"

#include "version.h"

#ifndef UINT64_MAX
	#define UINT64_MAX		18446744073709551615ULL
#endif
#ifndef INT64_MAX
	#define INT64_MAX		0x7fffffffffffffffLL
#endif

#ifndef CLUSTER_SLOTS
    #define CLUSTER_SLOTS 16384
#endif

#ifndef RAW_KEY_MIGRATING
    #define MIGRATION_PREFIX  "\xff\xff\xffMIGRATION_PREFIX"
#endif

#define SAFE_DELETE(x)\
do{\
    if(x != NULL) {\
        delete x;\
        x = NULL;\
    }\
}while(0)

#define KEY_HASH_SLOT(key) key_hash_slot((key).data(), (key).size(), CLUSTER_SLOTS)

static inline double millitime(){
	struct timeval now;
	gettimeofday(&now, NULL);
	double ret = now.tv_sec + now.tv_usec/1000.0/1000.0;
	return ret;
}

static inline int64_t time_ms(){
	struct timeval now;
	gettimeofday(&now, NULL);
	return now.tv_sec * 1000 + now.tv_usec/1000;
}

#endif

