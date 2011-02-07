/*************************************************************************************************
 * Common symbols for the library
 *                                                               Copyright (C) 2009-2011 FAL Labs
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


#ifndef _KCCOMMON_H                      // duplication check
#define _KCCOMMON_H

extern "C" {
#include <stdint.h>
}

#include <cassert>
#include <cctype>
#include <cerrno>
#include <cfloat>
#include <climits>
#include <clocale>
#include <cmath>
#include <csetjmp>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cstring>
#include <ctime>
#include <cwchar>

#include <stdexcept>
#include <exception>
#include <limits>
#include <new>
#include <typeinfo>

#include <utility>
#include <functional>
#include <memory>
#include <iterator>
#include <algorithm>
#include <locale>

#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <queue>

#include <ios>
#include <iostream>
#include <streambuf>
#include <fstream>
#include <sstream>

#if defined(_MSC_VER)
#define snprintf  _snprintf
#endif

#if defined(__CYGWIN__)
inline long double modfl(long double val, long double* iptr) {
  double integ;
  double fract = std::modf(val, &integ);
  *iptr = integ;
  return fract;
}
#endif

namespace std {
using ::modfl;
using ::snprintf;
}

#if __cplusplus > 199711L || defined(__GXX_EXPERIMENTAL_CXX0X__) || defined(_MSC_VER)

#include <unordered_map>
#include <unordered_set>

#else

#include <tr1/unordered_map>
#include <tr1/unordered_set>

namespace std {
using tr1::hash;
using tr1::unordered_map;
using tr1::unordered_set;
}

#endif

#undef VERSION
#undef LIBVER
#undef LIBREV
#undef SYSNAME
#undef BIGEND
#undef CLOCKTICK
#undef PAGESIZE

#undef SUCCESS
#undef NOIMPL
#undef INVALID
#undef NOREPOS
#undef NOPERM
#undef BROKEN
#undef DUPREC
#undef NOREC
#undef LOGIC
#undef SYSTEM
#undef MISC

#undef DEBUG
#undef INFO
#undef WARN
#undef ERROR
#undef OPEN
#undef CLOSE
#undef CLEAR
#undef ITERATE
#undef SYNCHRONIZE
#undef BEGINTRAN
#undef COMMITTRAN
#undef ABORTTRAN

#if defined(_KCUYIELD)
#if defined(_MSC_VER)
#include <windows.h>
#define _yield_()  ::Sleep(0)
#else
#include <sched.h>
#define _yield_()  ::sched_yield()
#endif
#define _testyield_()                           \
  do {                                          \
    static uint32_t _KC_seed = 725;             \
    _KC_seed = _KC_seed * 123456761 + 211;      \
    if (_KC_seed % 0x100 == 0) _yield_();       \
  } while(false)
#define _assert_(KC_a)                          \
  do {                                          \
    _testyield_();                              \
    assert(KC_a);                               \
  } while(false)
#elif defined(_KCDEBUG)
#define _yield_()
#define _testyield_()
#define _assert_(KC_a)  assert(KC_a)
#else
#define _yield_()                        ///< for debugging
#define _testyield_()                    ///< for debugging
#define _assert_(KC_a)                   ///< for debugging
#endif

#if defined(__GNUC__)
#define __KCFUNC__  __func__             ///< for debugging
#elif defined(_MSC_VER)
#define __KCFUNC__  __FUNCTION__         ///< for debugging
#else
#define __KCFUNC__  "-"                  ///< for debugging
#endif
#define _KCCODELINE_  __FILE__, __LINE__, __KCFUNC__  ///< for debugging

/**
 * All symbols of Kyoto Cabinet.
 */
namespace kyotocabinet {
const int8_t INT8MAX = (std::numeric_limits<int8_t>::max)();       ///< maximum value of int8_t
const int16_t INT16MAX = (std::numeric_limits<int16_t>::max)();    ///< maximum value of int16_t
const int32_t INT32MAX = (std::numeric_limits<int32_t>::max)();    ///< maximum value of int32_t
const int64_t INT64MAX = (std::numeric_limits<int64_t>::max)();    ///< maximum value of int64_t
const int8_t INT8MIN = (std::numeric_limits<int8_t>::min)();       ///< minimum value of int8_t
const int16_t INT16MIN = (std::numeric_limits<int16_t>::min)();    ///< minimum value of int16_t
const int32_t INT32MIN = (std::numeric_limits<int32_t>::min)();    ///< minimum value of int32_t
const int64_t INT64MIN = (std::numeric_limits<int64_t>::min)();    ///< minimum value of int64_t
const uint8_t UINT8MAX = (std::numeric_limits<uint8_t>::max)();    ///< maximum value of uint8_t
const uint16_t UINT16MAX = (std::numeric_limits<uint16_t>::max)(); ///< maximum value of uint16_t
const uint32_t UINT32MAX = (std::numeric_limits<uint32_t>::max)(); ///< maximum value of uint32_t
const uint64_t UINT64MAX = (std::numeric_limits<uint64_t>::max)(); ///< maximum value of uint64_t
}


#endif                                   // duplication check

// END OF FILE
