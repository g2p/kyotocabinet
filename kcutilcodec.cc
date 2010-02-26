/*************************************************************************************************
 * Popular encoders and decoders
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


#include "cmdcommon.h"


// global variables
const char* g_progname;                  // program name


// function prototypes
int main(int argc, char** argv);
static void usage();
static int runconf(int argc, char** argv);
static int32_t procconf(int32_t mode);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "conf")) {
    rv = runconf(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
    rv = 0;
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: popular encoders and decoders of Kyoto Cabinet\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s conf [-v|-i|-l|-p]\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// parse arguments of conf command
static int runconf(int argc, char** argv) {
  int32_t mode = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-v")) {
        mode = 'v';
      } else if (!std::strcmp(argv[i], "-i")) {
        mode = 'i';
      } else if (!std::strcmp(argv[i], "-l")) {
        mode = 'l';
      } else if (!std::strcmp(argv[i], "-p")) {
        mode = 'p';
      } else {
        usage();
      }
    } else {
      usage();
    }
  }
  int rv = procconf(mode);
  return rv;
}


// perform conf command
static int32_t procconf(int32_t mode) {
  switch (mode) {
    case 'v': {
      iprintf("%s\n", kc::VERSION);
      break;
    }
    case 'i': {
      iprintf("%s\n", _KC_APPINC);
      break;
    }
    case 'l': {
      iprintf("%s\n", _KC_APPLIBS);
      break;
    }
    case 'p': {
      iprintf("%s\n", _KC_BINDIR);
      break;
    }
    default: {
      iprintf("VERSION: %s\n", kc::VERSION);
      iprintf("LIBVER: %d\n", kc::LIBVER);
      iprintf("LIBREV: %d\n", kc::LIBREV);
      iprintf("FMTVER: %d\n", kc::FMTVER);
      iprintf("SYSNAME: %s\n", kc::SYSNAME);
      iprintf("BIGEND: %d\n", kc::BIGEND);
      iprintf("CLOCKTICK: %d\n", kc::CLOCKTICK);
      iprintf("PAGESIZE: %d\n", kc::PAGESIZE);
      iprintf("TYPES: void*=%d short=%d int=%d long=%d long_long=%d size_t=%d"
              " float=%d double=%d long_double=%d\n",
              (int)sizeof(void*), (int)sizeof(short), (int)sizeof(int), (int)sizeof(long),
              (int)sizeof(long long), (int)sizeof(size_t),
              (int)sizeof(float), (int)sizeof(double), (int)sizeof(long double));
      iprintf("prefix: %s\n", _KC_PREFIX);
      iprintf("includedir: %s\n", _KC_INCLUDEDIR);
      iprintf("libdir: %s\n", _KC_LIBDIR);
      iprintf("bindir: %s\n", _KC_BINDIR);
      iprintf("libexecdir: %s\n", _KC_LIBEXECDIR);
      iprintf("appinc: %s\n", _KC_APPINC);
      iprintf("applibs: %s\n", _KC_APPLIBS);
      break;
    }
  }
  return 0;
}



// END OF FILE
