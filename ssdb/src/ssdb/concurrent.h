/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_CONCURRENT_H_
#define SSDB_CONCURRENT_H_

#include <pthread.h>
#include "../util/crc16.h"

class DBKeyLock {
private:
	// key lock
	void *locks;
	uint32_t count;

	// mdl
	pthread_rwlock_t mdlock;

public:
	DBKeyLock(uint32_t concurrency=1024) {
		// alloc space
		this->count = concurrency;
		this->locks = (void *)malloc(count * sizeof(pthread_mutex_t));

		// init row locks
		pthread_mutex_t *mutex = (pthread_mutex_t *)this->locks;
		for (uint32_t i = 0; i < this->count; ++i) {
			pthread_mutex_init(mutex, NULL);
			++mutex;
		}

		// init mdl
		pthread_rwlock_init(&this->mdlock, NULL);
	}
	~DBKeyLock() {
		if (this->locks) {
			pthread_mutex_t *mutex = (pthread_mutex_t *)this->locks;
			for (uint32_t i = 0; i < this->count; ++i) {
				pthread_mutex_destroy(mutex);
				mutex++;
			}
			free(this->locks);
		}
	}

public:
	void lock_db() {
		pthread_rwlock_wrlock(&this->mdlock);
	}

	void unlock_db() {
		pthread_rwlock_unlock(&this->mdlock);
	}

	void lock(const std::string &key) {
		// lock mdl first
		pthread_rwlock_rdlock(&this->mdlock);

		pthread_mutex_t *mutex = get_lock(key);
		pthread_mutex_lock(mutex);
	}

	void unlock(const std::string &key) {
		pthread_mutex_t *mutex = get_lock(key);
		pthread_mutex_unlock(mutex);

		// unlock mdl last
		pthread_rwlock_unlock(&this->mdlock);
	}

private:
	pthread_mutex_t *get_lock(const std::string &key) {
		uint16_t slot = crc16(key.data(), key.size()) % this->count;
		pthread_mutex_t *mutex = (pthread_mutex_t *)this->locks;
		mutex += slot;
		return mutex;
	}
};

#endif
