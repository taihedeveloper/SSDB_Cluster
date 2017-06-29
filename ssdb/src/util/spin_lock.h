#ifndef UTIL_SPIN_RWLOCK_H_
#define UTIL_SPIN_RWLOCK_H_

#include <map>
#include <set>
#include <string>
#include <pthread.h>
#include "hash.h"

enum LockMode {
	INVALID_LOCK = 0,
	READ_LOCK,
	WRITE_LOCK
};

class RWLock {
	private:
		pthread_rwlock_t rwlock;
	public:
		RWLock();
		~RWLock();
	bool Lock(LockMode mode);
	bool Unlock(LockMode mode);
};

class KeyLock : public RWLock {
	public:
		KeyLock() {}
		~KeyLock() {}
		void add_key(std::string key) {
			dict.insert(key);
		}
		void del_key(std::string key) {
			dict.erase(key);
		}
		bool test_key(std::string key) {
			return dict.count(key) != 0;
		}
    private:
		std::set<std::string> dict;
};

class SegKeyLock {
#define KEY_LOCK_SEG_DEGREE 4
#define KEY_LOCK_SEG (1<<KEY_LOCK_SEG_DEGREE)

public:
	SegKeyLock() {}
	~SegKeyLock() {}

public:
	KeyLock & get_key_lock(const std::string& key) {
		uint32_t idx = string_hash(key) >> (32-KEY_LOCK_SEG_DEGREE);
		return m_locks[idx];
	}

	void add_key(const std::string &key) {
		KeyLock &lock = get_key_lock(key);
		lock.add_key(key);
	}

	void del_key(const std::string &key) {
		KeyLock &lock = get_key_lock(key);
		lock.del_key(key);
	}

	bool test_key(const std::string &key) {
		KeyLock &lock = get_key_lock(key);
		return lock.test_key(key);
	}

public:
	void Lock(LockMode mode) {
		for (int i = 0; i < KEY_LOCK_SEG; i++) {
			m_locks[i].Lock(mode);
		}
	}
	void Unlock(LockMode mode) {
		for (int i = 0; i < KEY_LOCK_SEG; i++) {
			m_locks[i].Unlock(mode);
		}
	}

private:
	static uint32_t string_hash(const std::string& key) {
		return ssdb::str_hash(key.c_str(), key.size());
	}

private:
	KeyLock m_locks[KEY_LOCK_SEG];
};

class SegRWLock {
#define RWLOCK_SEG_DEGREE 4
#define RWLOCK_SEG (1<<RWLOCK_SEG_DEGREE)

public:
	SegRWLock() {}
	~SegRWLock() {}

	RWLock &get_rw_lock(int64_t slot) {
		return m_locks[slot % RWLOCK_SEG];
	}

private:
	RWLock m_locks[RWLOCK_SEG];
};

class SpinRWLock {
	public:
		volatile uint32_t m_lock;

		SpinRWLock();
		~SpinRWLock();
 		bool Lock(LockMode mode);
 		bool Trylock(LockMode mode);
 		bool Unlock(LockMode mode);
};

class SpinMutexLock {
	public:
		volatile uint32_t m_lock;
		SpinMutexLock();
		~SpinMutexLock();
		bool Lock();
		bool Unlock();
};

template<typename T>
class ReadLockGuard {
	public:
		ReadLockGuard(T &lock) : m_lock_impl(lock) {
			m_lock_impl.Lock(READ_LOCK);
		}
		~ReadLockGuard() {
			m_lock_impl.Unlock(READ_LOCK);
		}
	private:
		T &m_lock_impl;
};

template<typename T>
class WriteLockGuard {
	public:
		WriteLockGuard(T &lock) : m_lock_impl(lock) {
			m_lock_impl.Lock(WRITE_LOCK);
		}
		~WriteLockGuard() {
			m_lock_impl.Unlock(WRITE_LOCK);
		}
	private:
		T &m_lock_impl;
};

template<typename T>
class LockGuard {
	public:
		LockGuard(T &lock) : m_lock_impl(lock) {
			m_lock_impl.Lock();
		}
		~LockGuard() {
			m_lock_impl.Unlock();
		}
	private:
		T &m_lock_impl;
};
#endif
