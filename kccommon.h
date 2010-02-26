/*************************************************************************************************
 * Common symbols for the library
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


#ifndef _KCCOMMON_H                      // duplication check
#define _KCCOMMON_H

#define __STDC_LIMIT_MACROS 1

extern "C" {
#include <inttypes.h>
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

#include <exception>
#include <stdexcept>
#include <new>
#include <iterator>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <map>
#include <utility>
#include <algorithm>
#include <functional>
#include <iostream>
#include <fstream>

namespace std {
using ::nan;
using ::modfl;
}

#include <tr1/unordered_set>
#include <tr1/unordered_map>

namespace std {
using tr1::hash;
using tr1::unordered_map;
using tr1::unordered_set;
}


#endif                                   // duplication check

// END OF FILE
