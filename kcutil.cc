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
const char* VERSION = _KC_VERSION;


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
const int32_t CLOCKTICK = sysconf(_SC_CLK_TCK);


/** The size of a page. */
const int32_t PAGESIZE = sysconf(_SC_PAGESIZE);


/**
 * Allocate a nullified region on memory.
 */
void* mapalloc(size_t size) {
#if defined(_SYS_LINUX_)
  void* ptr = ::mmap(0, sizeof(size) + size,
                     PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) throw std::bad_alloc();
  *(size_t*)ptr = size;
  return (char*)ptr + sizeof(size);
#else
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
  size_t size = *((size_t*)ptr - 1);
  ::munmap((char*)ptr - sizeof(size), sizeof(size) + size);
#else
  std::free(ptr);
#endif
}


/**
 * Get the time of day in seconds.
 * @return the time of day in seconds.  The accuracy is in microseconds.
 */
double time() {
  struct ::timeval tv;
  if (::gettimeofday(&tv, NULL) != 0) return 0.0;
  return tv.tv_sec + tv.tv_usec / 1000000.0;
}


}                                        // common namespace

// END OF FILE
