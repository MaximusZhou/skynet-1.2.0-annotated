#ifndef SKYNET_RWLOCK_H
#define SKYNET_RWLOCK_H

#ifndef USE_PTHREAD_LOCK

// 每个读写锁对应的结构体
struct rwlock {
	int write;
	int read;
};

static inline void
rwlock_init(struct rwlock *lock) {
	lock->write = 0;
	lock->read = 0;
}

static inline void
rwlock_rlock(struct rwlock *lock) {
	for (;;) {
		// 在等待写锁被释放了
		while(lock->write) {
			__sync_synchronize();
		}
		__sync_add_and_fetch(&lock->read,1);
		// 在运行过程中，有线程成功加上写锁了，则重新设置lock->read
		// 从这里也可以看出，写锁的优先级比读锁优先级高
		// 加读锁的过程中，发现有写锁，则回退，即重新设置lock->read的值
		if (lock->write) {
			__sync_sub_and_fetch(&lock->read,1);
		} else {
			// 表示此时肯定没有写锁了，后面写锁要加成功，必须等到lock->read值为0
			break;
		}
	}
}

static inline void
rwlock_wlock(struct rwlock *lock) {
	while (__sync_lock_test_and_set(&lock->write,1)) {}
	// 在等待读锁
	while(lock->read) {
		// 发出一个full barrier
		__sync_synchronize();
	}

	// 跑到这个地方表示, 一定没有读和写锁了
}

static inline void
rwlock_wunlock(struct rwlock *lock) {
	__sync_lock_release(&lock->write);
}

static inline void
rwlock_runlock(struct rwlock *lock) {
	// 释放读锁，即减少相应读锁的计数
	__sync_sub_and_fetch(&lock->read,1);
}

#else

#include <pthread.h>

// only for some platform doesn't have __sync_*
// todo: check the result of pthread api

struct rwlock {
	pthread_rwlock_t lock;
};

static inline void
rwlock_init(struct rwlock *lock) {
	pthread_rwlock_init(&lock->lock, NULL);
}

static inline void
rwlock_rlock(struct rwlock *lock) {
	 pthread_rwlock_rdlock(&lock->lock);
}

static inline void
rwlock_wlock(struct rwlock *lock) {
	 pthread_rwlock_wrlock(&lock->lock);
}

static inline void
rwlock_wunlock(struct rwlock *lock) {
	pthread_rwlock_unlock(&lock->lock);
}

static inline void
rwlock_runlock(struct rwlock *lock) {
	pthread_rwlock_unlock(&lock->lock);
}

#endif

#endif
