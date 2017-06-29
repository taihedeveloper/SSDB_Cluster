
#include <sched.h>
#include "atomic.h"
#include "spin_lock.h"

RWLock::RWLock() {
	pthread_rwlock_init(&rwlock, NULL);
}

RWLock::~RWLock() {
	pthread_rwlock_destroy(&rwlock);
}

bool RWLock::Lock(LockMode mode) {
	if(mode == READ_LOCK) {
		return pthread_rwlock_rdlock(&rwlock) == 0;
	} else {
		return pthread_rwlock_wrlock(&rwlock) == 0;
	}
}

bool RWLock::Unlock(LockMode mode) {
	return pthread_rwlock_unlock(&rwlock) == 0;
}

SpinRWLock::SpinRWLock() : m_lock(0) {

}

SpinRWLock::~SpinRWLock() {

}

bool SpinRWLock::Lock(LockMode mode) {
	if(mode == READ_LOCK) {
		while(true) {
			while(m_lock & 0xfff00000) {
				sched_yield();
			}
			if((0xfff00000 & atomic_add_uint32(&m_lock, 1)) == 0) {
				return true;
			}
			atomic_sub_uint32(&m_lock, 1);
		}
	} else if(mode == WRITE_LOCK) {
		while(true) {
			while(m_lock & 0xfff00000) {
				sched_yield();
			}
			if((0xfff00000 & atomic_add_uint32(&m_lock, 0x100000)) == 0x100000) {
				while(m_lock & 0x000fffff) {
					sched_yield();
				}
				return true;
			}
			atomic_sub_uint32(&m_lock, 0x100000);
		}
	} else {
		return false;
	}
}

bool SpinRWLock::Trylock(LockMode mode) {
	if(mode == READ_LOCK) {
		if(m_lock & 0xfff00000) {
			return false;
		}
		if((0xfff00000 & atomic_add_uint32(&m_lock, 1)) == 0) {
			return true;
		}
		atomic_sub_uint32(&m_lock, 1);
		return false;
	} else if(mode == WRITE_LOCK) {
		if(m_lock & 0xfff00000) {
			return false;
		}
		if((0xfff00000 & atomic_add_uint32(&m_lock, 1)) == 0) {
			if(m_lock & 0x000fffff) {
				return false;
			}
			return true;
		}
		atomic_sub_uint32(&m_lock, 0x100000);
		return false;
	} else {
		return false;
	}
}

bool SpinRWLock::Unlock(LockMode mode) {
	if(mode == READ_LOCK) {
		atomic_sub_uint32(&m_lock, 1);
		return true;
	} else if(mode == WRITE_LOCK) {
		atomic_sub_uint32(&m_lock, 0x100000);
		return true;
	}
	return false;
}


SpinMutexLock::SpinMutexLock() : m_lock(0) {

}

SpinMutexLock::~SpinMutexLock() {

}

bool SpinMutexLock::Lock() {
	while(!atomic_cmp_set_uint32(&m_lock, 0, 1)) {
		sched_yield();
	}
	return true;
}

bool SpinMutexLock::Unlock() {
	m_lock = 0;
	return true;
}
