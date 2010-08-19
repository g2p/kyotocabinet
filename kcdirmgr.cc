/*************************************************************************************************
 * The command line utility of the directory hash database
 *                                                               Copyright (C) 2009-2010 FAL Labs
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


#include <kcdirdb.h>
#include "cmdcommon.h"


// global variables
const char* g_progname;                  // program name


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kc::BasicDB* db, const char* info);
static void ebufprint(std::ostringstream* ebuf);
static int32_t runcreate(int argc, char** argv);
static int32_t runinform(int argc, char** argv);
static int32_t runset(int argc, char** argv);
static int32_t runremove(int argc, char** argv);
static int32_t runget(int argc, char** argv);
static int32_t runlist(int argc, char** argv);
static int32_t runimport(int argc, char** argv);
static int32_t rundump(int argc, char** argv);
static int32_t runload(int argc, char** argv);
static int32_t runcheck(int argc, char** argv);
static int32_t proccreate(const char* path, int32_t oflags, int32_t opts);
static int32_t procinform(const char* path, int32_t oflags, bool st);
static int32_t procset(const char* path, const char* kbuf, size_t ksiz,
                       const char* vbuf, size_t vsiz, int32_t oflags, int32_t mode);
static int32_t procremove(const char* path, const char* kbuf, size_t ksiz, int32_t oflags);
static int32_t procget(const char* path, const char* kbuf, size_t ksiz,
                       int32_t oflags, bool px, bool pz);
static int32_t proclist(const char* path, const char*kbuf, size_t ksiz, int32_t oflags,
                        int64_t max, bool pv, bool px);
static int32_t procimport(const char* path, const char* file, int32_t oflags, bool sx);
static int32_t procdump(const char* path, const char* file, int32_t oflags);
static int32_t procload(const char* path, const char* file, int32_t oflags);
static int32_t proccheck(const char* path, int32_t oflags);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "create")) {
    rv = runcreate(argc, argv);
  } else if (!std::strcmp(argv[1], "inform")) {
    rv = runinform(argc, argv);
  } else if (!std::strcmp(argv[1], "set")) {
    rv = runset(argc, argv);
  } else if (!std::strcmp(argv[1], "remove")) {
    rv = runremove(argc, argv);
  } else if (!std::strcmp(argv[1], "get")) {
    rv = runget(argc, argv);
  } else if (!std::strcmp(argv[1], "list")) {
    rv = runlist(argc, argv);
  } else if (!std::strcmp(argv[1], "import")) {
    rv = runimport(argc, argv);
  } else if (!std::strcmp(argv[1], "dump")) {
    rv = rundump(argc, argv);
  } else if (!std::strcmp(argv[1], "load")) {
    rv = runload(argc, argv);
  } else if (!std::strcmp(argv[1], "check")) {
    rv = runcheck(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: the command line utility of the directory hash database of Kyoto Cabinet\n",
          g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s create [-otr] [-onl|-otl|-onr] [-tc] path\n", g_progname);
  eprintf("  %s inform [-onl|-otl|-onr] [-st] path\n", g_progname);
  eprintf("  %s set [-onl|-otl|-onr] [-add|-app|-inci|-incd] [-sx] path key value\n",
          g_progname);
  eprintf("  %s remove [-onl|-otl|-onr] [-sx] path key\n", g_progname);
  eprintf("  %s get [-onl|-otl|-onr] [-sx] [-px] [-pz] path key\n", g_progname);
  eprintf("  %s list [-onl|-otl|-onr] [-max num] [-sx] [-pv] [-px] path [key]\n", g_progname);
  eprintf("  %s import [-onl|-otl|-onr] [-sx] path [file]\n", g_progname);
  eprintf("  %s dump [-onl|-otl|-onr] path [file]\n", g_progname);
  eprintf("  %s load [-otr] [-onl|-otl|-onr] path [file]\n", g_progname);
  eprintf("  %s check [-onl|-otl|-onr] path\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print error message of database
static void dberrprint(kc::BasicDB* db, const char* info) {
  kc::BasicDB::Error err = db->error();
  eprintf("%s: %s: %s: %d: %s: %s\n",
          g_progname, info, db->path().c_str(), err.code(), err.name(), err.message());
}


// print the content of the error buffer
static void ebufprint(std::ostringstream* ebuf) {
  const std::string& str = ebuf->str();
  std::vector<std::string> lines;
  kc::strsplit(str, '\n', &lines);
  std::vector<std::string>::iterator it = lines.begin();
  std::vector<std::string>::iterator itend = lines.end();
  while (it != itend) {
    if (!it->empty()) eprintf("%s: %s\n", g_progname, it->c_str());
    it++;
  }
  ebuf->str("");
}


// parse arguments of create command
static int32_t runcreate(int argc, char** argv) {
  const char* path = NULL;
  int32_t oflags = 0;
  int32_t opts = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-otr")) {
        oflags |= kc::DirDB::OTRUNCATE;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-tc")) {
        opts |= kc::DirDB::TCOMPRESS;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = proccreate(path, oflags, opts);
  return rv;
}


// parse arguments of inform command
static int32_t runinform(int argc, char** argv) {
  const char* path = NULL;
  int32_t oflags = 0;
  bool st = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-st")) {
        st = true;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procinform(path, oflags, st);
  return rv;
}


// parse arguments of set command
static int32_t runset(int argc, char** argv) {
  const char* path = NULL;
  const char* kstr = NULL;
  const char* vstr = NULL;
  int32_t oflags = 0;
  int32_t mode = 0;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-add")) {
        mode = 'a';
      } else if (!std::strcmp(argv[i], "-app")) {
        mode = 'c';
      } else if (!std::strcmp(argv[i], "-inci")) {
        mode = 'i';
      } else if (!std::strcmp(argv[i], "-incd")) {
        mode = 'd';
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else if (!vstr) {
      vstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !kstr || !vstr) usage();
  char* kbuf;
  size_t ksiz;
  char* vbuf;
  size_t vsiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
    vbuf = kc::hexdecode(vstr, &vsiz);
    vstr = vbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
    vsiz = std::strlen(vstr);
    vbuf = NULL;
  }
  int32_t rv = procset(path, kstr, ksiz, vstr, vsiz, oflags, mode);
  delete[] kbuf;
  delete[] vbuf;
  return rv;
}


// parse arguments of remove command
static int32_t runremove(int argc, char** argv) {
  const char* path = NULL;
  const char* kstr = NULL;
  int32_t oflags = 0;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  int32_t rv = procremove(path, kstr, ksiz, oflags);
  delete[] kbuf;
  return rv;
}


// parse arguments of get command
static int32_t runget(int argc, char** argv) {
  const char* path = NULL;
  const char* kstr = NULL;
  int32_t oflags = 0;
  bool sx = false;
  bool px = false;
  bool pz = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-pz")) {
        pz = true;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  int32_t rv = procget(path, kstr, ksiz, oflags, px, pz);
  delete[] kbuf;
  return rv;
}


// parse arguments of list command
static int32_t runlist(int argc, char** argv) {
  const char* path = NULL;
  const char* kstr = NULL;
  int32_t oflags = 0;
  int64_t max = -1;
  bool sx = false;
  bool pv = false;
  bool px = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-max")) {
        if (++i >= argc) usage();
        max = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-pv")) {
        pv = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  char* kbuf = NULL;
  size_t ksiz = 0;
  if (kstr) {
    if (sx) {
      kbuf = kc::hexdecode(kstr, &ksiz);
      kstr = kbuf;
    } else {
      ksiz = std::strlen(kstr);
      kbuf = new char[ksiz+1];
      std::memcpy(kbuf, kstr, ksiz);
      kbuf[ksiz] = '\0';
    }
  }
  int32_t rv = proclist(path, kbuf, ksiz, oflags, max, pv, px);
  delete[] kbuf;
  return rv;
}


// parse arguments of import command
static int32_t runimport(int argc, char** argv) {
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procimport(path, file, oflags, sx);
  return rv;
}


// parse arguments of dump command
static int32_t rundump(int argc, char** argv) {
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procdump(path, file, oflags);
  return rv;
}


// parse arguments of load command
static int32_t runload(int argc, char** argv) {
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-otr")) {
        oflags |= kc::DirDB::OTRUNCATE;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procload(path, file, oflags);
  return rv;
}


// parse arguments of check command
static int32_t runcheck(int argc, char** argv) {
  const char* path = NULL;
  int32_t oflags = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!path && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::DirDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::DirDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::DirDB::ONOREPAIR;
      } else {
        usage();
      }
    } else if (!path) {
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = proccheck(path, oflags);
  return rv;
}


// perform create command
static int32_t proccreate(const char* path, int32_t oflags, int32_t opts) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (opts > 0) db.tune_options(opts);
  if (!db.open(path, kc::DirDB::OWRITER | kc::DirDB::OCREATE | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform inform command
static int32_t procinform(const char* path, int32_t oflags, bool st) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OREADER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  if (st) {
    std::map<std::string, std::string> status;
    status["opaque"] = "";
    if (db.status(&status)) {
      uint32_t type = kc::atoi(status["type"].c_str());
      iprintf("type: %s (%s) (type=0x%02X)\n",
              kc::BasicDB::typecname(type), kc::BasicDB::typestring(type), type);
      uint32_t rtype = kc::atoi(status["realtype"].c_str());
      if (rtype > 0 && rtype != type)
        iprintf("real type: %s (%s) (realtype=0x%02X)\n",
                kc::BasicDB::typecname(rtype), kc::BasicDB::typestring(rtype), rtype);
      uint32_t chksum = kc::atoi(status["chksum"].c_str());
      iprintf("format version: %s (libver=%s.%s) (chksum=0x%02X)\n", status["fmtver"].c_str(),
              status["libver"].c_str(), status["librev"].c_str(), chksum);
      iprintf("path: %s\n", status["path"].c_str());
      int32_t flags = kc::atoi(status["flags"].c_str());
      iprintf("status flags:");
      if (flags & kc::DirDB::FOPEN) iprintf(" open");
      if (flags & kc::DirDB::FFATAL) iprintf(" fatal");
      iprintf(" (flags=%d)", flags);
      if (kc::atoi(status["recovered"].c_str()) > 0) iprintf(" (recovered)");
      if (kc::atoi(status["reorganized"].c_str()) > 0) iprintf(" (reorganized)");
      iprintf("\n", flags);
      int32_t opts = kc::atoi(status["opts"].c_str());
      iprintf("options:");
      if (opts & kc::DirDB::TSMALL) iprintf(" small");
      if (opts & kc::DirDB::TLINEAR) iprintf(" linear");
      if (opts & kc::DirDB::TCOMPRESS) iprintf(" compress");
      iprintf(" (opts=%d)\n", opts);
      if (status["opaque"].size() >= 16) {
        const char* opaque = status["opaque"].c_str();
        iprintf("opaque:");
        if (std::count(opaque, opaque + 16, 0) != 16) {
          for (int32_t i = 0; i < 16; i++) {
            iprintf(" %02X", ((unsigned char*)opaque)[i]);
          }
        } else {
          iprintf(" 0");
        }
        iprintf("\n");
      }
      int64_t count = kc::atoi(status["count"].c_str());
      std::string cntstr = unitnumstr(count);
      iprintf("count: %lld (%S)\n", count, &cntstr);
      int64_t size = kc::atoi(status["size"].c_str());
      std::string sizestr = unitnumstrbyte(size);
      iprintf("size: %lld (%S)\n", size, &sizestr);
    }
  } else {
    iprintf("count: %lld\n", (long long)db.count());
    iprintf("size: %lld\n", (long long)db.size());
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform set command
static int32_t procset(const char* path, const char* kbuf, size_t ksiz,
                       const char* vbuf, size_t vsiz, int32_t oflags, int32_t mode) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OWRITER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  switch (mode) {
    default: {
      if (!db.set(kbuf, ksiz, vbuf, vsiz)) {
        ebufprint(&ebuf);
        dberrprint(&db, "DB::set failed");
        err = true;
      }
      break;
    }
    case 'a': {
      if (!db.add(kbuf, ksiz, vbuf, vsiz)) {
        ebufprint(&ebuf);
        dberrprint(&db, "DB::add failed");
        err = true;
      }
      break;
    }
    case 'c': {
      if (!db.append(kbuf, ksiz, vbuf, vsiz)) {
        ebufprint(&ebuf);
        dberrprint(&db, "DB::append failed");
        err = true;
      }
      break;
    }
    case 'i': {
      int64_t onum = db.increment(kbuf, ksiz, kc::atoi(vbuf));
      if (onum == INT64_MIN) {
        ebufprint(&ebuf);
        dberrprint(&db, "DB::increment failed");
        err = true;
      } else {
        iprintf("%lld\n", (long long)onum);
      }
      break;
    }
    case 'd': {
      double onum = db.increment(kbuf, ksiz, kc::atof(vbuf));
      if (kc::chknan(onum)) {
        ebufprint(&ebuf);
        dberrprint(&db, "DB::increment failed");
        err = true;
      } else {
        iprintf("%f\n", onum);
      }
      break;
    }
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform remove command
static int32_t procremove(const char* path, const char* kbuf, size_t ksiz, int32_t oflags) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OWRITER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  if (!db.remove(kbuf, ksiz)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::remove failed");
    err = true;
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform get command
static int32_t procget(const char* path, const char* kbuf, size_t ksiz,
                       int32_t oflags, bool px, bool pz) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OREADER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  size_t vsiz;
  char* vbuf = db.get(kbuf, ksiz, &vsiz);
  if (vbuf) {
    printdata(vbuf, vsiz, px);
    if (!pz) iprintf("\n");
    delete[] vbuf;
  } else {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::get failed");
    err = true;
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform list command
static int32_t proclist(const char* path, const char*kbuf, size_t ksiz, int32_t oflags,
                        int64_t max, bool pv, bool px) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OREADER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  class VisitorImpl : public kc::DB::Visitor {
  public:
    explicit VisitorImpl(bool pv, bool px) : pv_(pv), px_(px) {}
  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      printdata(kbuf, ksiz, px_);
      if (pv_) {
        iprintf("\t");
        printdata(vbuf, vsiz, px_);
      }
      iprintf("\n");
      return NOP;
    }
    bool pv_;
    bool px_;
  } visitor(pv, px);
  if (kbuf || max >= 0) {
    if (max < 0) max = INT64_MAX;
    kc::DirDB::Cursor cur(&db);
    if (kbuf) {
      if (!cur.jump(kbuf, ksiz) && db.error() != kc::BasicDB::Error::NOREC) {
        ebufprint(&ebuf);
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    } else {
      if (!cur.jump() && db.error() != kc::BasicDB::Error::NOREC) {
        ebufprint(&ebuf);
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    }
    while (!err && max > 0) {
      if (!cur.accept(&visitor, false, true)) {
        if (db.error() != kc::BasicDB::Error::NOREC) {
          ebufprint(&ebuf);
          dberrprint(&db, "Cursor::accept failed");
          err = true;
        }
        break;
      }
      max--;
    }
  } else {
    if (!db.iterate(&visitor, false)) {
      ebufprint(&ebuf);
      dberrprint(&db, "DB::iterate failed");
      err = true;
    }
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform import command
static int32_t procimport(const char* path, const char* file, int32_t oflags, bool sx) {
  std::istream *is = &std::cin;
  std::ifstream ifs;
  if (file) {
    ifs.open(file, std::ios_base::in | std::ios_base::binary);
    if (!ifs) {
      eprintf("%s: %s: open error\n", g_progname, file);
      return 1;
    }
    is = &ifs;
  }
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OWRITER | kc::DirDB::OCREATE | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  int64_t cnt = 0;
  std::string line;
  std::vector<std::string> fields;
  while (!err && getline(is, &line)) {
    cnt++;
    kc::strsplit(line, '\t', &fields);
    if (sx) {
      std::vector<std::string>::iterator it = fields.begin();
      std::vector<std::string>::iterator itend = fields.end();
      while (it != itend) {
        size_t esiz;
        char* ebuf = kc::hexdecode(it->c_str(), &esiz);
        it->clear();
        it->append(ebuf, esiz);
        delete[] ebuf;
        it++;
      }
    }
    switch (fields.size()) {
      case 2: {
        if (!db.set(fields[0], fields[1])) {
          ebufprint(&ebuf);
          dberrprint(&db, "DB::set failed");
          err = true;
        }
        break;
      }
      case 1: {
        if (!db.remove(fields[0]) && db.error() != kc::BasicDB::Error::NOREC) {
          ebufprint(&ebuf);
          dberrprint(&db, "DB::remove failed");
          err = true;
        }
        break;
      }
    }
    iputchar('.');
    if (cnt % 50 == 0) iprintf(" (%d)\n", cnt);
  }
  if (cnt % 50 > 0) iprintf(" (%d)\n", cnt);
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform dump command
static int32_t procdump(const char* path, const char* file, int32_t oflags) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OREADER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  if (file) {
    if (!db.dump_snapshot(file)) {
      ebufprint(&ebuf);
      dberrprint(&db, "DB::dump_snapshot");
      err = true;
    }
  } else {
    if (!db.dump_snapshot(&std::cout)) {
      ebufprint(&ebuf);
      dberrprint(&db, "DB::dump_snapshot");
      err = true;
    }
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform load command
static int32_t procload(const char* path, const char* file, int32_t oflags) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OWRITER | kc::DirDB::OCREATE | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  if (file) {
    if (!db.load_snapshot(file)) {
      ebufprint(&ebuf);
      dberrprint(&db, "DB::load_snapshot");
      err = true;
    }
  } else {
    if (!db.load_snapshot(&std::cin)) {
      ebufprint(&ebuf);
      dberrprint(&db, "DB::load_snapshot");
      err = true;
    }
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform check command
static int32_t proccheck(const char* path, int32_t oflags) {
  kc::DirDB db;
  std::ostringstream ebuf;
  db.tune_error_reporter(&ebuf, false);
  if (!db.open(path, kc::DirDB::OREADER | oflags)) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  ebufprint(&ebuf);
  bool err = false;
  kc::DirDB::Cursor cur(&db);
  if (!cur.jump() && db.error() != kc::BasicDB::Error::NOREC) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::jump failed");
    err = true;
  }
  int64_t cnt = 0;
  while (!err) {
    size_t ksiz;
    const char* vbuf;
    size_t vsiz;
    char* kbuf = cur.get(&ksiz, &vbuf, &vsiz);
    if (kbuf) {
      cnt++;
      size_t rsiz;
      char* rbuf = db.get(kbuf, ksiz, &rsiz);
      if (rbuf) {
        if (rsiz != vsiz || std::memcmp(rbuf, vbuf, rsiz)) {
          ebufprint(&ebuf);
          dberrprint(&db, "DB::get failed");
          err = true;
        }
        delete[] rbuf;
      } else {
        ebufprint(&ebuf);
        dberrprint(&db, "DB::get failed");
        err = true;
      }
      delete[] kbuf;
      if (cnt % 1000 == 0) {
        iputchar('.');
        if (cnt % 50000 == 0) iprintf(" (%lld)\n", (long long)cnt);
      }
    } else {
      if (db.error() != kc::BasicDB::Error::NOREC) {
        ebufprint(&ebuf);
        dberrprint(&db, "Cursor::get failed");
        err = true;
      }
      break;
    }
    if (!cur.step() && db.error() != kc::BasicDB::Error::NOREC) {
      ebufprint(&ebuf);
      dberrprint(&db, "Cursor::step failed");
      err = true;
    }
  }
  iprintf(" (end)\n");
  if (db.count() != cnt) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::count failed");
    err = true;
  }
  if (!db.close()) {
    ebufprint(&ebuf);
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (!err) iprintf("%lld records were checked successfully\n", (long long)cnt);
  return err ? 1 : 0;
}



// END OF FILE
