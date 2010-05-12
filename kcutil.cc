/*************************************************************************************************
 * Utility functions
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


#include "kcutil.h"
#include "myconf.h"

namespace kyotocabinet {                 // common namespace


/** The package version. */
const char* const VERSION = _KC_VERSION;


/** The library version. */
const int32_t LIBVER = _KC_LIBVER;


/** The library revision. */
const int32_t LIBREV = _KC_LIBREV;


/** The database format version. */
const int32_t FMTVER = _KC_FMTVER;


/** The system name. */
const char* SYSNAME = _KC_SYSNAME;


/** The flag for big endian environments. */
const bool BIGEND = _KC_BIGEND ? true : false;


/** The clock tick of interruption. */
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
const int32_t CLOCKTICK = 100;
#else
const int32_t CLOCKTICK = sysconf(_SC_CLK_TCK);
#endif


/** The size of a page. */
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
static int32_t win_getpagesize() {
  ::SYSTEM_INFO ibuf;
  ::GetSystemInfo(&ibuf);
  return ibuf.dwPageSize;
}
const int32_t PAGESIZE = win_getpagesize();
#else
const int32_t PAGESIZE = sysconf(_SC_PAGESIZE);
#endif


/**
 * Allocate a nullified region on memory.
 */
void* mapalloc(size_t size) {
#if defined(_SYS_LINUX_)
  _assert_(size > 0 && size <= MEMMAXSIZ);
  void* ptr = ::mmap(0, sizeof(size) + size,
                     PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) throw std::bad_alloc();
  *(size_t*)ptr = size;
  return (char*)ptr + sizeof(size);
#else
  _assert_(size > 0 && size <= MEMMAXSIZ);
  void* ptr = std::calloc(size, 1);
  if (!ptr) throw std::bad_alloc();
  return ptr;
#endif
}


/**
 * Free a region on memory.
 */
void mapfree(void* ptr) {
#if defined(_SYS_LINUX_)
  _assert_(ptr);
  size_t size = *((size_t*)ptr - 1);
  ::munmap((char*)ptr - sizeof(size), sizeof(size) + size);
#else
  _assert_(ptr);
  std::free(ptr);
#endif
}


/**
 * Get the time of day in seconds.
 * @return the time of day in seconds.  The accuracy is in microseconds.
 */
double time() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::FILETIME ft;
  ::GetSystemTimeAsFileTime(&ft);
  ::LARGE_INTEGER li;
  li.LowPart = ft.dwLowDateTime;
  li.HighPart = ft.dwHighDateTime;
  return li.QuadPart / 10000000.0;
#else
  _assert_(true);
  struct ::timeval tv;
  if (::gettimeofday(&tv, NULL) != 0) return 0.0;
  return tv.tv_sec + tv.tv_usec / 1000000.0;
#endif
}


/**
 * Get the process ID.
 */
int64_t getpid() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  return ::_getpid();
#else
  _assert_(true);
  return ::getpid();
#endif
}


/**
 * Get the value of an environment variable.
 */
const char* getenv(const char* name) {
  _assert_(name);
  return ::getenv(name);
}



}                                        // common namespace

// END OF FILE
