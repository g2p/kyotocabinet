/*************************************************************************************************
 * Threading devices
 *                                                      Copyright (C) 2009-2010 Mikio Hirabayashi
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#include "kcthread.h"
#include "myconf.h"

namespace kyotocabinet {                 // common namespace


/**
 * Thread internal.
 */
struct ThreadCore {
  ::pthread_t th;                        ///< identifier
  bool alive;                            ///< alive flag
};


/**
 * Call the running thread.
 * @param arg the thread.
 * @return always NULL.
 */
static void* threadrun(void* arg);


/**
 * Default constructor.
 */
Thread::Thread() : opq_(NULL) {
  ThreadCore* core = new ThreadCore;
  core->alive = false;
  opq_ = (void*)core;
}


/**
 * Destructor.
 */
Thread::~Thread() {
  ThreadCore* core = (ThreadCore*)opq_;
  if (core->alive) join();
  delete core;
}


/**
 * Start the thread.
 */
void Thread::start() {
  ThreadCore* core = (ThreadCore*)opq_;
  if (core->alive) throw std::invalid_argument("already started");
  if (::pthread_create(&core->th, NULL, threadrun, this) != 0)
    throw std::runtime_error("pthread_create");
  core->alive = true;
}


/**
 * Wait for the thread to finish.
 */
void Thread::join() {
  ThreadCore* core = (ThreadCore*)opq_;
  if (!core->alive) throw std::invalid_argument("not alive");
  core->alive = false;
  if (::pthread_join(core->th, NULL) != 0) throw std::runtime_error("pthread_join");
}


/**
 * Put the thread in the detached state.
 */
void Thread::detach() {
  ThreadCore* core = (ThreadCore*)opq_;
  if (!core->alive) throw std::invalid_argument("not alive");
  core->alive = false;
  if (::pthread_detach(core->th) != 0) throw std::runtime_error("pthread_detach");
}


/**
 * Yield the processor from the running thread.
 */
void Thread::yield() {
  ::sched_yield();
}


/**
 * Terminate the running thread.
 */
void Thread::exit() {
  ::pthread_exit(NULL);
}


/**
 * Suspend execution of the current thread.
 */
bool Thread::sleep(double sec) {
  if (!std::isnormal(sec) || sec <= 0.0) return false;
  double integ, fract;
  fract = std::modf(sec, &integ);
  struct ::timespec req, rem;
  req.tv_sec = integ;
  req.tv_nsec = fract * 1000000000;
  if (req.tv_nsec > 999999999) req.tv_nsec = 999999999;
  while (::nanosleep(&req, &rem) != 0) {
    if (errno != EINTR) return false;
    req = rem;
  }
  return true;
}


/**
 * Get the hash value of the current thread.
 */
int64_t Thread::hash() {
  pthread_t tid = pthread_self();
  int64_t num;
  if (sizeof(tid) == sizeof(num)) {
    std::memcpy(&num, &tid, sizeof(num));
  } else if (sizeof(tid) == sizeof(int32_t)) {
    uint32_t inum;
    std::memcpy(&inum, &tid, sizeof(inum));
    num = inum;
  } else {
    num = hashmurmur(&tid, sizeof(tid));
  }
  return num & INT64_MAX;
}


/**
 * Call the running thread.
 */
static void* threadrun(void* arg) {
  Thread* thread = (Thread*)arg;
  thread->run();
  return NULL;
}


/**
 * Default constructor.
 */
Mutex::Mutex() : opq_(NULL) {
  ::pthread_mutex_t* mutex = new ::pthread_mutex_t;
  if (::pthread_mutex_init(mutex, NULL) != 0) {
    delete mutex;
    throw std::runtime_error("pthread_mutex_init");
  }
  opq_ = (void*)mutex;
}


/**
 * Constructor with the specifications.
 */
Mutex::Mutex(Type type) {
  ::pthread_mutexattr_t attr;
  if (::pthread_mutexattr_init(&attr) != 0) throw std::runtime_error("pthread_mutexattr_init");
  switch (type) {
    case FAST: {
      break;
    }
    case ERRORCHECK: {
      if (::pthread_mutexattr_settype(&attr, ::PTHREAD_MUTEX_ERRORCHECK) != 0)
        throw std::runtime_error("pthread_mutexattr_settype");
      break;
    }
    case RECURSIVE: {
      if (::pthread_mutexattr_settype(&attr, ::PTHREAD_MUTEX_RECURSIVE) != 0)
        throw std::runtime_error("pthread_mutexattr_settype");
      break;
    }
  }
  ::pthread_mutex_t* mutex = new ::pthread_mutex_t;
  if (::pthread_mutex_init(mutex, &attr) != 0) {
    ::pthread_mutexattr_destroy(&attr);
    delete mutex;
    throw std::runtime_error("pthread_mutex_init");
  }
  ::pthread_mutexattr_destroy(&attr);
  opq_ = (void*)mutex;
}


/**
 * Destructor.
 */
Mutex::~Mutex() {
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  ::pthread_mutex_destroy(mutex);
  delete mutex;
}


/**
 * Get the lock.
 */
void Mutex::lock() {
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  if (::pthread_mutex_lock(mutex) != 0) throw std::runtime_error("pthread_mutex_lock");
}


/**
 * Try to get the lock.
 */
bool Mutex::lock_try() {
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  int32_t ecode = ::pthread_mutex_trylock(mutex);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_mutex_trylock");
 return false;
}


/**
 * Try to get the lock.
 */
bool Mutex::lock_try(double sec) {
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  struct ::timeval tv;
  struct ::timespec ts;
  if (::gettimeofday(&tv, NULL) == 0) {
    double integ;
    double fract = std::modf(sec, &integ);
    ts.tv_sec = tv.tv_sec + integ;
    ts.tv_nsec = tv.tv_usec * 1000.0 + fract * 1000000000.0;
    if (ts.tv_nsec >= 1000000000) {
      ts.tv_nsec -= 1000000000;
      ts.tv_sec++;
    }
  } else {
    ts.tv_sec = std::time(NULL) + 1;
    ts.tv_nsec = 0;
  }
  int32_t ecode = ::pthread_mutex_timedlock(mutex, &ts);
  if (ecode == 0) return true;
  if (ecode != ETIMEDOUT) throw std::runtime_error("pthread_mutex_timedlock");
 return false;
}


/**
 * Release the lock.
 */
void Mutex::unlock() {
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  if (::pthread_mutex_unlock(mutex) != 0) throw std::runtime_error("pthread_mutex_unlock");
}


/**
 * Default constructor.
 */
SpinLock::SpinLock() : opq_(NULL) {
#if defined(_KC_GCCATOMIC)
#else
  ::pthread_spinlock_t* spin = new ::pthread_spinlock_t;
  if (::pthread_spin_init(spin, ::PTHREAD_PROCESS_PRIVATE) != 0) {
    delete spin;
    throw std::runtime_error("pthread_spin_init");
  }
  opq_ = (void*)spin;
#endif
}


/**
 * Destructor.
 */
SpinLock::~SpinLock() {
#if defined(_KC_GCCATOMIC)
#else
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  ::pthread_spin_destroy(spin);
  delete spin;
#endif
}


/**
 * Get the lock.
 */
void SpinLock::lock() {
#if defined(_KC_GCCATOMIC)
  while (!__sync_bool_compare_and_swap(&opq_, 0, 1)) {
    ::sched_yield();
  }
#else
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  if (::pthread_spin_lock(spin) != 0) throw std::runtime_error("pthread_spin_lock");
#endif
}


/**
 * Try to get the lock.
 */
bool SpinLock::lock_try() {
#if defined(_KC_GCCATOMIC)
  return __sync_bool_compare_and_swap(&opq_, 0, 1);
#else
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  int32_t ecode = ::pthread_spin_trylock(spin);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_spin_trylock");
  return false;
#endif
}


/**
 * Release the lock.
 */
void SpinLock::unlock() {
#if defined(_KC_GCCATOMIC)
  (void)__sync_lock_test_and_set(&opq_, 0);
#else
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  if (::pthread_spin_unlock(spin) != 0) throw std::runtime_error("pthread_spin_unlock");
#endif
}


/**
 * Default constructor.
 */
RWLock::RWLock() : opq_(NULL) {
  ::pthread_rwlock_t* rwlock = new ::pthread_rwlock_t;
  if (::pthread_rwlock_init(rwlock, NULL) != 0) {
    delete rwlock;
    throw std::runtime_error("pthread_rwlock_init");
  }
  opq_ = (void*)rwlock;
}


/**
 * Destructor.
 */
RWLock::~RWLock() {
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  ::pthread_rwlock_destroy(rwlock);
  delete rwlock;
}


/**
 * Get the writer lock.
 */
void RWLock::lock_writer() {
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  if (::pthread_rwlock_wrlock(rwlock) != 0) throw std::runtime_error("pthread_rwlock_lock");
}


/**
 * Try to get the writer lock.
 */
bool RWLock::lock_writer_try() {
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  int32_t ecode = ::pthread_rwlock_trywrlock(rwlock);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_rwlock_trylock");
 return false;
}


/**
 * Get a reader lock.
 */
void RWLock::lock_reader() {
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  if (::pthread_rwlock_rdlock(rwlock) != 0) throw std::runtime_error("pthread_rwlock_lock");
}


/**
 * Try to get a reader lock.
 */
bool RWLock::lock_reader_try() {
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  int32_t ecode = ::pthread_rwlock_tryrdlock(rwlock);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_rwlock_trylock");
 return false;
}


/**
 * Release the lock.
 */
void RWLock::unlock() {
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  if (::pthread_rwlock_unlock(rwlock) != 0) throw std::runtime_error("pthread_rwlock_unlock");
}


/**
 * SpinRWLock internal.
 */
struct SpinRWLockCore {
#if defined(_KC_GCCATOMIC)
  int32_t sem;                           ///< semaphore
  int32_t wc;                            ///< count of writers
  int32_t rc;                            ///< count of readers
#else
  ::pthread_spinlock_t sem;              ///< semaphore
  int32_t wc;                            ///< count of writers
  int32_t rc;                            ///< count of readers
#endif
};


/**
 * Lock the semephore of SpinRWLock.
 * @param core the internal fields.
 */
static void spinrwlocklock(SpinRWLockCore* core);


/**
 * Unlock the semephore of SpinRWLock.
 * @param core the internal fields.
 */
static void spinrwlockunlock(SpinRWLockCore* core);


/**
 * Default constructor.
 */
SpinRWLock::SpinRWLock() : opq_(NULL) {
#if defined(_KC_GCCATOMIC)
  SpinRWLockCore* core = new SpinRWLockCore;
  core->sem = 0;
  core->wc = 0;
  core->rc = 0;
  opq_ = (void*)core;
#else
  SpinRWLockCore* core = new SpinRWLockCore;
  if (::pthread_spin_init(&core->sem, ::PTHREAD_PROCESS_PRIVATE) != 0)
    throw std::runtime_error("pthread_spin_init");
  core->wc = 0;
  core->rc = 0;
  opq_ = (void*)core;
#endif
}


/**
 * Destructor.
 */
SpinRWLock::~SpinRWLock() {
#if defined(_KC_GCCATOMIC)
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  delete core;
#else
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  ::pthread_spin_destroy(&core->sem);
  delete core;
#endif
}


/**
 * Get the writer lock.
 */
void SpinRWLock::lock_writer() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  while (core->wc > 0 || core->rc > 0) {
    spinrwlockunlock(core);
    ::sched_yield();
    spinrwlocklock(core);
  }
  core->wc++;
  spinrwlockunlock(core);
}


/**
 * Try to get the writer lock.
 */
bool SpinRWLock::lock_writer_try() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->wc > 0 || core->rc > 0) {
    spinrwlockunlock(core);
    return false;
  }
  spinrwlockunlock(core);
 return true;
}


/**
 * Get a reader lock.
 */
void SpinRWLock::lock_reader() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  while (core->wc > 0) {
    spinrwlockunlock(core);
    ::sched_yield();
    spinrwlocklock(core);
  }
  core->rc++;
  spinrwlockunlock(core);
}


/**
 * Try to get a reader lock.
 */
bool SpinRWLock::lock_reader_try() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->wc > 0) {
    spinrwlockunlock(core);
    return false;
  }
  core->rc++;
  spinrwlockunlock(core);
  return true;
}


/**
 * Release the lock.
 */
void SpinRWLock::unlock() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->wc > 0) {
    core->wc--;
  } else {
    core->rc--;
  }
  spinrwlockunlock(core);
}


/**
 * Promote a reader lock to the writer lock.
 */
bool SpinRWLock::promote() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->rc > 1) {
    spinrwlockunlock(core);
    return false;
  }
  core->rc--;
  core->wc++;
  spinrwlockunlock(core);
  return true;
}


/**
 * Demote the writer lock to a reader lock.
 */
void SpinRWLock::demote() {
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  core->wc--;
  core->rc++;
  spinrwlockunlock(core);
}


/**
 * Lock the semephore of SpinRWLock.
 */
static void spinrwlocklock(SpinRWLockCore* core) {
#if defined(_KC_GCCATOMIC)
  while (!__sync_bool_compare_and_swap(&core->sem, 0, 1)) {
    ::sched_yield();
  }
#else
  if (::pthread_spin_lock(&core->sem) != 0) throw std::runtime_error("pthread_spin_lock");
#endif
}


/**
 * Unlock the semephore of SpinRWLock.
 */
static void spinrwlockunlock(SpinRWLockCore* core) {
#if defined(_KC_GCCATOMIC)
  (void)__sync_lock_test_and_set(&core->sem, 0);
#else
  if (::pthread_spin_unlock(&core->sem) != 0) throw std::runtime_error("pthread_spin_unlock");
#endif
}


/**
 * Default constructor.
 */
CondVar::CondVar() : opq_(NULL) {
  ::pthread_cond_t* cond = new ::pthread_cond_t;
  if (::pthread_cond_init(cond, NULL) != 0) {
    delete cond;
    throw std::runtime_error("pthread_cond_init");
  }
  opq_ = (void*)cond;
}


/**
 * Destructor.
 */
CondVar::~CondVar() {
  ::pthread_cond_t* cond = (::pthread_cond_t*)opq_;
  ::pthread_cond_destroy(cond);
  delete cond;
}


/**
 * Wait for the signal.
 */
void CondVar::wait(Mutex* mutex) {
  ::pthread_cond_t* cond = (::pthread_cond_t*)opq_;
  ::pthread_mutex_t* mymutex = (::pthread_mutex_t*)mutex->opq_;
  if (::pthread_cond_wait(cond, mymutex) != 0) throw std::runtime_error("pthread_cond_wait");
}


/**
 * Wait for the signal.
 */
bool CondVar::wait(Mutex* mutex, double sec) {
  if (sec <= 0) return false;
  ::pthread_cond_t* cond = (::pthread_cond_t*)opq_;
  ::pthread_mutex_t* mymutex = (::pthread_mutex_t*)mutex->opq_;
  struct ::timeval tv;
  struct ::timespec ts;
  if (::gettimeofday(&tv, NULL) == 0) {
    double integ;
    double fract = std::modf(sec, &integ);
    ts.tv_sec = tv.tv_sec + integ;
    ts.tv_nsec = tv.tv_usec * 1000.0 + fract * 1000000000.0;
    if (ts.tv_nsec >= 1000000000) {
      ts.tv_nsec -= 1000000000;
      ts.tv_sec++;
    }
  } else {
    ts.tv_sec = std::time(NULL) + 1;
    ts.tv_nsec = 0;
  }
  int32_t ecode = ::pthread_cond_timedwait(cond, mymutex, &ts);
  if (ecode == 0) return true;
  if (ecode != ETIMEDOUT && ecode != EINTR) throw std::runtime_error("pthread_cond_timedwait");
  return false;
}


/**
 * Send the wake-up signal to another waiting thread.
 */
void CondVar::signal() {
  ::pthread_cond_t* cond = (::pthread_cond_t*)opq_;
  if (::pthread_cond_signal(cond) != 0) throw std::runtime_error("pthread_cond_signal");
}


/**
 * Send the wake-up signal to all waiting threads.
 */
void CondVar::broadcast() {
  ::pthread_cond_t* cond = (::pthread_cond_t*)opq_;
  if (::pthread_cond_broadcast(cond) != 0) throw std::runtime_error("pthread_cond_broadcast");
}


/**
 * Default constructor.
 */
TSDKey::TSDKey() : opq_(NULL) {
  ::pthread_key_t* key = new ::pthread_key_t;
  if (::pthread_key_create(key, NULL) != 0) {
    delete key;
    throw std::runtime_error("pthread_key_create");
  }
  opq_ = (void*)key;
}


/**
 * Constructor with the specifications.
 */
TSDKey::TSDKey(void (*dstr)(void*)) : opq_(NULL) {
  ::pthread_key_t* key = new ::pthread_key_t;
  if (::pthread_key_create(key, dstr) != 0) {
    delete key;
    throw std::runtime_error("pthread_key_create");
  }
  opq_ = (void*)key;
}


/**
 * Destructor.
 */
TSDKey::~TSDKey() {
  ::pthread_key_t* key = (::pthread_key_t*)opq_;
  ::pthread_key_delete(*key);
  delete key;
}


/**
 * Set the value.
 */
void TSDKey::set(void* ptr) {
  ::pthread_key_t* key = (::pthread_key_t*)opq_;
  if (::pthread_setspecific(*key, ptr) != 0) throw std::runtime_error("pthread_setspecific");
}


/**
 * Get the value.
 */
void* TSDKey::get() const {
  ::pthread_key_t* key = (::pthread_key_t*)opq_;
  return ::pthread_getspecific(*key);
}


/**
 * Set the new value.
 */
int64_t AtomicInt64::set(int64_t val) {
#if defined(_KC_GCCATOMIC)
  int64_t oval = __sync_lock_test_and_set(&value_, val);
  __sync_synchronize();
  return oval;
#else
  lock_.lock();
  int64_t oval = value_;
  value_ = val;
  lock_.unlock();
  return oval;
#endif
}


/**
 * Add a value.
 */
int64_t AtomicInt64::add(int64_t val) {
#if defined(_KC_GCCATOMIC)
  int64_t oval = __sync_fetch_and_add(&value_, val);
  __sync_synchronize();
  return oval;
#else
  lock_.lock();
  int64_t oval = value_;
  value_ += val;
  lock_.unlock();
  return oval;
#endif
}


/**
 * Perform compare-and-swap.
 */
bool AtomicInt64::cas(int64_t oval, int64_t nval) {
#if defined(_KC_GCCATOMIC)
  bool rv = __sync_bool_compare_and_swap(&value_, oval, nval);
  __sync_synchronize();
  return rv;
#else
  bool rv = false;
  lock_.lock();
  if (value_ == oval) {
    value_ = nval;
    rv = true;
  }
  lock_.unlock();
  return rv;
#endif
}


/**
 * Get the current value.
 */
int64_t AtomicInt64::get() const {
#if defined(_KC_GCCATOMIC)
  __sync_synchronize();
  return value_;
#else
  lock_.lock();
  int64_t oval = value_;
  lock_.unlock();
  return oval;
#endif
}


}                                        // common namespace

// END OF FILE
