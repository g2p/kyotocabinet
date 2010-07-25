/*************************************************************************************************
 * Polymorphic database
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


#ifndef _KCPOLYDB_H                      // duplication check
#define _KCPOLYDB_H

#include <kccommon.h>
#include <kcutil.h>
#include <kcdb.h>
#include <kcthread.h>
#include <kcfile.h>
#include <kccompress.h>
#include <kccompare.h>
#include <kcmap.h>
#include <kcprotodb.h>
#include <kccachedb.h>
#include <kchashdb.h>
#include <kctreedb.h>
#include <kcdirdb.h>

namespace kyotocabinet {                 // common namespace


/**
 * Polymorphic database.
 */
class PolyDB : public FileDB {
public:
  class Cursor;
public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor : public FileDB::Cursor {
    friend class PolyDB;
  public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(PolyDB* db) : db_(db), cur_(NULL) {
      _assert_(db);
      if (db_->type_ == TYPEVOID) {
        ProtoTreeDB tmpdb;
        cur_ = tmpdb.cursor();
      } else {
        cur_ = db->db_->cursor();
      }
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      _assert_(true);
      delete cur_;
    }
    /**
     * Accept a visitor to the current record.
     * @param visitor a visitor object.
     * @param writable true for writable operation, or false for read-only operation.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return true on success, or false on failure.
     * @note the operation for each record is performed atomically and other threads accessing
     * the same record are blocked.
     */
    bool accept(Visitor* visitor, bool writable = true, bool step = false) {
      _assert_(visitor);
      if (db_->type_ == TYPEVOID) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      return cur_->accept(visitor, writable, step);
    }
    /**
     * Jump the cursor to the first record.
     * @return true on success, or false on failure.
     */
    bool jump() {
      _assert_(true);
      if (db_->type_ == TYPEVOID) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      return cur_->jump();
    }
    /**
     * Jump the cursor onto a record.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     */
    bool jump(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= MEMMAXSIZ);
      if (db_->type_ == TYPEVOID) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      return cur_->jump(kbuf, ksiz);
    }
    /**
     * Jump the cursor to a record.
     * @note Equal to the original Cursor::jump method except that the parameter is std::string.
     */
    bool jump(const std::string& key) {
      _assert_(true);
      if (db_->type_ == TYPEVOID) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      return jump(key.c_str(), key.size());
    }
    /**
     * Step the cursor to the next record.
     * @return true on success, or false on failure.
     */
    bool step() {
      _assert_(true);
      if (db_->type_ == TYPEVOID) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      return cur_->step();
    }
    /**
     * Get the database object.
     * @return the database object.
     */
    PolyDB* db() {
      _assert_(true);
      return db_;
    }
  private:
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    PolyDB* db_;
    /** The inner cursor. */
    FileDB::Cursor* cur_;
  };
  /**
   * Default constructor.
   */
  explicit PolyDB() : type_(TYPEVOID), db_(NULL), error_(), zcomp_(NULL) {
    _assert_(true);
  }
  /**
   * Constructor.
   * @param db the internal database object.  Its possession is transferred inside and the
   * object is deleted automatically.
   */
  explicit PolyDB(FileDB* db) : type_(TYPEMISC), db_(db), error_(), zcomp_(NULL) {
    _assert_(db);
  }
  /**
   * Destructor.
   * @note If the database is not closed, it is closed implicitly.
   */
  virtual ~PolyDB() {
    _assert_(true);
    if (type_ != TYPEVOID) close();
    delete zcomp_;
  }
  /**
   * Accept a visitor to a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   * @note the operation for each record is performed atomically and other threads accessing the
   * same record are blocked.
   */
  bool accept(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable = true) {
    _assert_(kbuf && ksiz <= MEMMAXSIZ && visitor);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->accept(kbuf, ksiz, visitor, writable);
  }
  /**
   * Iterate to accept a visitor for each record.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   * @note the whole iteration is performed atomically and other threads are blocked.
   */
  bool iterate(Visitor *visitor, bool writable = true) {
    _assert_(visitor);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->iterate(visitor, writable);
  }
  /**
   * Get the last happened error.
   * @return the last happened error.
   */
  Error error() const {
    _assert_(true);
    if (type_ == TYPEVOID) return error_;
    return db_->error();
  }
  /**
   * Set the error information.
   * @param code an error code.
   * @param message a supplement message.
   */
  void set_error(Error::Code code, const char* message) {
    _assert_(message);
    if (type_ == TYPEVOID) {
      error_->set(code, message);
      return;
    }
    db_->set_error(code, message);
  }
  /**
   * Open a database file.
   * @param path the path of a database file.  If it is "-", the database will be a prototype
   * hash database.  If it is "+", the database will be a prototype tree database.  If it is
   * "*", the database will be a cache database.  If its suffix is ".kch", the database will be
   * a file hash database.  If its suffix is ".kct", the database will be a file tree database.
   * If its suffix is ".kcd", the database will be a directory database.  Otherwise, this
   * function fails.  Tuning parameters can trail the name, separated by "#".  Each parameter is
   * composed of the name and the value, separated by "=".  If the "type" parameter is specified,
   * the database type is determined by the value in "-", "+", "*", "kch", "kct", and "kcd".  The
   * prototype hash database and the prototype tree database do not support any other tuning
   * parameter.  The cache database supports "bnum", "capcount", and "capsize".  The file hash
   * database supports "apow", "fpow", "opts", "bnum", "msiz", "dfunit", "zcomp", "erstrm",
   * "ervbs", and "zkey".  The file tree database supports all parameters of the file hash
   * database and "psiz", "rcomp", "pccap" in addition.  The directory database supports "opts",
   * "zcomp", and "zkey".
   * @param mode the connection mode.  PolyDB::OWRITER as a writer, PolyDB::OREADER as a
   * reader.  The following may be added to the writer mode by bitwise-or: PolyDB::OCREATE,
   * which means it creates a new database if the file does not exist, PolyDB::OTRUNCATE, which
   * means it creates a new database regardless if the file exists, PolyDB::OAUTOTRAN, which
   * means each updating operation is performed in implicit transaction, PolyDB::OAUTOSYNC,
   * which means each updating operation is followed by implicit synchronization with the file
   * system.  The following may be added to both of the reader mode and the writer mode by
   * bitwise-or: PolyDB::ONOLOCK, which means it opens the database file without file locking,
   * PolyDB::OTRYLOCK, which means locking is performed without blocking, PolyDB::ONOREPAIR,
   * which means the database file is not repaired implicitly even if file destruction is
   * detected.
   * @return true on success, or false on failure.
   * @note The tuning parameter "bnum" corresponds to the original "tune_bucket" method.
   * "capcount" is for "cap_count".  "capsize" is for "cap_size".  "apow" is for
   * "tune_alignment".  "fpow" is for "tune_fbp".  "opts" is for "tune_options" and the value
   * can contain "s" for the small option, "l" for the linear option, and "c" for the compress
   * option.  "msiz" is for "tune_map".  "dfunit" is for "tune_defrag".  "zcomp" is for
   * "tune_compressor" and the value can be "zlib" for the Zlib raw compressor, "def" for the
   * Zlib deflate compressor, "gz" for the Zlib gzip compressor, or "arc" for the Arcfour cipher.
   * "erstrm" and "ervbs" are for "tune_error_reporter" and the formar value can be "stdout" or
   * "stderr" and the latter value can be "true" or "false".  "zkey" specifies the cipher key of
   * the compressor.  "psiz" is for "tune_page".  "rcomp" is for "tune_comparator" and the value
   * can be "lex" for the lexical comparator or "dec" for the decimal comparator.  "pccap" is for
   * "tune_page_cache".  Every opened database must be closed by the PolyDB::close method when it
   * is no longer in use.  It is not allowed for two or more database objects in the same process
   * to keep their connections to the same database file at the same time.
   */
  bool open(const std::string& path, uint32_t mode = OWRITER | OCREATE) {
    _assert_(true);
    if (type_ == TYPEMISC) return db_->open(path, mode);
    if (type_ != TYPEVOID) {
      set_error(Error::INVALID, "already opened");
      return false;
    }
    std::vector<std::string> elems;
    strsplit(path, '#', &elems);
    std::string fpath;
    Type type = TYPEVOID;
    int64_t bnum = -1;
    int64_t capcount = -1;
    int64_t capsize = -1;
    int32_t apow = -1;
    int32_t fpow = -1;
    bool tsmall = false;
    bool tlinear = false;
    bool tcompress = false;
    int64_t msiz = -1;
    int64_t dfunit = -1;
    Compressor *zcomp = NULL;
    int64_t psiz = -1;
    Comparator *rcomp = NULL;
    int64_t pccap = 0;
    std::ostream* erstrm = NULL;
    bool ervbs = false;
    std::string zkey = "";
    ArcfourCompressor *arccomp = NULL;
    std::vector<std::string>::iterator it = elems.begin();
    std::vector<std::string>::iterator itend = elems.end();
    if (it != itend) {
      fpath = *it;
      it++;
    }
    const char* fstr = fpath.c_str();
    const char* pv = std::strrchr(fstr, File::PATHCHR);
    if (pv) fstr = pv + 1;
    if (!std::strcmp(fstr, "-")) {
      type = TYPEPHASH;
    } else if (!std::strcmp(fstr, "+")) {
      type = TYPEPTREE;
    } else if (!std::strcmp(fstr, "*")) {
      type = TYPECACHE;
    } else {
      pv = std::strrchr(fstr, File::EXTCHR);
      if (pv) {
        pv++;
        if (!std::strcmp(pv, "kch") || !std::strcmp(pv, "hdb")) {
          type = TYPEHASH;
        } else if (!std::strcmp(pv, "kct") || !std::strcmp(pv, "tdb")) {
          type = TYPETREE;
        } else if (!std::strcmp(pv, "kcd") || !std::strcmp(pv, "ddb")) {
          type = TYPEDIR;
        }
      }
    }
    while (it != itend) {
      std::vector<std::string> fields;
      if (strsplit(*it, '=', &fields) > 1) {
        const char* key = fields[0].c_str();
        const char* value = fields[1].c_str();
        if (!std::strcmp(key, "type")) {
          if (!std::strcmp(value, "-") || !std::strcmp(value, "phash")) {
            type = TYPEPHASH;
          } else if (!std::strcmp(value, "+") || !std::strcmp(value, "ptree")) {
            type = TYPEPTREE;
          } else if (!std::strcmp(value, "*") || !std::strcmp(value, "cache")) {
            type = TYPECACHE;
          } else if (!std::strcmp(value, "kch") || !std::strcmp(value, "hdb") ||
                     !std::strcmp(value, "hash")) {
            type = TYPEHASH;
          } else if (!std::strcmp(value, "kct") || !std::strcmp(value, "tdb") ||
                     !std::strcmp(value, "tree")) {
            type = TYPETREE;
          } else if (!std::strcmp(value, "kcd") || !std::strcmp(value, "ddb") ||
                     !std::strcmp(value, "dir")) {
            type = TYPEDIR;
          }
        } else if (!std::strcmp(key, "bnum") || !std::strcmp(key, "buckets")) {
          bnum = atoix(value);
        } else if (!std::strcmp(key, "capcount") || !std::strcmp(key, "cap_count")) {
          capcount = atoix(value);
        } else if (!std::strcmp(key, "capsize") || !std::strcmp(key, "cap_size")) {
          capsize = atoix(value);
        } else if (!std::strcmp(key, "apow") || !std::strcmp(key, "alignment")) {
          apow = atoix(value);
        } else if (!std::strcmp(key, "fpow") || !std::strcmp(key, "fbp")) {
          fpow = atoix(value);
        } else if (!std::strcmp(key, "opts") || !std::strcmp(key, "options")) {
          if (std::strchr(value, 's')) tsmall = true;
          if (std::strchr(value, 'l')) tlinear = true;
          if (std::strchr(value, 'c')) tcompress = true;
        } else if (!std::strcmp(key, "msiz") || !std::strcmp(key, "map")) {
          msiz = atoix(value);
        } else if (!std::strcmp(key, "dfunit") || !std::strcmp(key, "defrag")) {
          dfunit = atoix(value);
        } else if (!std::strcmp(key, "zcomp") || !std::strcmp(key, "compressor")) {
          delete zcomp;
          zcomp = NULL;
          arccomp = NULL;
          if (!std::strcmp(value, "zlib") || !std::strcmp(value, "raw")) {
            zcomp = new ZlibCompressor<Zlib::RAW>;
          } else if (!std::strcmp(value, "def") || !std::strcmp(value, "deflate")) {
            zcomp = new ZlibCompressor<Zlib::DEFLATE>;
          } else if (!std::strcmp(value, "gz") || !std::strcmp(value, "gzip")) {
            zcomp = new ZlibCompressor<Zlib::GZIP>;
          } else if (!std::strcmp(value, "arc") || !std::strcmp(value, "rc4")) {
            arccomp = new ArcfourCompressor();
            zcomp = arccomp;
          }
        } else if (!std::strcmp(key, "psiz") || !std::strcmp(key, "page")) {
          psiz = atoix(value);
        } else if (!std::strcmp(key, "pccap") || !std::strcmp(key, "cache")) {
          pccap = atoix(value);
        } else if (!std::strcmp(key, "rcomp") || !std::strcmp(key, "comparator")) {
          if (!std::strcmp(value, "lex") || !std::strcmp(value, "lexical")) {
            rcomp = &LEXICALCOMP;
          } else if (!std::strcmp(value, "dec") || !std::strcmp(value, "decimal")) {
            rcomp = &DECIMALCOMP;
          }
        } else if (!std::strcmp(key, "erstrm") || !std::strcmp(key, "reporter")) {
          if (!std::strcmp(value, "stdout") || !std::strcmp(value, "cout") ||
              atoix(value) == 1) {
            erstrm = &std::cout;
          } else if (!std::strcmp(value, "stderr") || !std::strcmp(value, "cerr") ||
                     atoix(value) == 2) {
            erstrm = &std::cerr;
          }
        } else if (!std::strcmp(key, "ervbs") || !std::strcmp(key, "erv")) {
          ervbs = !std::strcmp(value, "true") || atoix(value) > 0;
        } else if (!std::strcmp(key, "zkey") || !std::strcmp(key, "pass") ||
                   !std::strcmp(key, "password")) {
          zkey = value;
        }
      }
      it++;
    }
    if (zcomp) {
      delete zcomp_;
      zcomp_ = zcomp;
      tcompress = true;
    }
    FileDB *db;
    switch (type) {
      default: {
        set_error(Error::INVALID, "unknown database type");
        return false;
      }
      case TYPEPHASH: {
        ProtoHashDB* phdb = new ProtoHashDB();
        db = phdb;
        break;
      }
      case TYPEPTREE: {
        ProtoTreeDB *ptdb = new ProtoTreeDB();
        db = ptdb;
        break;
      }
      case TYPECACHE: {
        CacheDB* cdb = new CacheDB();
        if (bnum > 0) cdb->tune_buckets(bnum);
        if (capcount > 0) cdb->cap_count(capcount);
        if (capsize > 0) cdb->cap_size(capsize);
        db = cdb;
        break;
      }
      case TYPEHASH: {
        int8_t opts = 0;
        if (tsmall) opts |= HashDB::TSMALL;
        if (tlinear) opts |= HashDB::TLINEAR;
        if (tcompress) opts |= HashDB::TCOMPRESS;
        HashDB* hdb = new HashDB();
        if (apow >= 0) hdb->tune_alignment(apow);
        if (fpow >= 0) hdb->tune_fbp(fpow);
        if (opts > 0) hdb->tune_options(opts);
        if (bnum > 0) hdb->tune_buckets(bnum);
        if (msiz >= 0) hdb->tune_map(msiz);
        if (dfunit > 0) hdb->tune_defrag(dfunit);
        if (zcomp) hdb->tune_compressor(zcomp);
        if (erstrm) hdb->tune_error_reporter(erstrm, ervbs);
        db = hdb;
        break;
      }
      case TYPETREE: {
        int8_t opts = 0;
        if (tsmall) opts |= TreeDB::TSMALL;
        if (tlinear) opts |= TreeDB::TLINEAR;
        if (tcompress) opts |= TreeDB::TCOMPRESS;
        TreeDB* tdb = new TreeDB();
        if (apow >= 0) tdb->tune_alignment(apow);
        if (fpow >= 0) tdb->tune_fbp(fpow);
        if (opts > 0) tdb->tune_options(opts);
        if (bnum > 0) tdb->tune_buckets(bnum);
        if (psiz > 0) tdb->tune_page(psiz);
        if (msiz >= 0) tdb->tune_map(msiz);
        if (dfunit > 0) tdb->tune_defrag(dfunit);
        if (zcomp) tdb->tune_compressor(zcomp);
        if (pccap > 0) tdb->tune_page_cache(pccap);
        if (rcomp) tdb->tune_comparator(rcomp);
        if (erstrm) tdb->tune_error_reporter(erstrm, ervbs);
        db = tdb;
        break;
      }
      case TYPEDIR: {
        int8_t opts = 0;
        if (tcompress) opts |= DirDB::TCOMPRESS;
        DirDB* ddb = new DirDB();
        if (opts > 0) ddb->tune_options(opts);
        if (zcomp) ddb->tune_compressor(zcomp);
        db = ddb;
        break;
      }
    }
    if (arccomp) arccomp->set_key(zkey.c_str(), zkey.size());
    if (!db->open(fpath, mode)) {
      const Error& error = db->error();
      set_error(error.code(), error.message());
      delete db;
      return false;
    }
    if (arccomp) {
      const std::string& apath = File::absolute_path(fpath);
      uint64_t hash = (hashmurmur(apath.c_str(), apath.size()) >> 16) << 40;
      hash += (uint64_t)(time() * 256);
      arccomp->begin_cycle(hash);
    }
    type_ = type;
    db_ = db;
    return true;
  }
  /**
   * Close the database file.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    bool err = false;
    if (!db_->close()) {
      const Error& error = db_->error();
      set_error(error.code(), error.message());
      err = true;
    }
    delete zcomp_;
    delete db_;
    type_ = TYPEVOID;
    db_ = NULL;
    zcomp_ = NULL;
    return !err;
  }
  /**
   * Synchronize updated contents with the file and the device.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @param proc a postprocessor object.  If it is NULL, no postprocessing is performed.
   * @return true on success, or false on failure.
   */
  bool synchronize(bool hard = false, FileProcessor* proc = NULL) {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->synchronize(hard, proc);
  }
  /**
   * Begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction(bool hard = false) {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->begin_transaction(hard);
  }
  /**
   * Try to begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction_try(bool hard = false) {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->begin_transaction_try(hard);
  }
  /**
   * End transaction.
   * @param commit true to commit the transaction, or false to abort the transaction.
   * @return true on success, or false on failure.
   */
  bool end_transaction(bool commit = true) {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->end_transaction(commit);
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  bool clear() {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->clear();
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count() {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return -1;
    }
    return db_->count();
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  int64_t size() {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return -1;
    }
    return db_->size();
  }
  /**
   * Get the path of the database file.
   * @return the path of the database file, or an empty string on failure.
   */
  std::string path() {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return "";
    }
    return db_->path();
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    return db_->status(strmap);
  }
  /**
   * Create a cursor object.
   * @return the return value is the created cursor object.
   * @note Because the object of the return value is allocated by the constructor, it should be
   * released with the delete operator when it is no longer in use.
   */
  Cursor* cursor() {
    _assert_(true);
    return new Cursor(this);
  }
  /**
   * Reveal the inner database object.
   * @return the inner database object, or NULL on failure.
   */
  FileDB* reveal_inner_db() {
    _assert_(true);
    if (type_ == TYPEVOID) {
      set_error(Error::INVALID, "not opened");
      return NULL;
    }
    return db_;
  }
private:
  /** Dummy constructor to forbid the use. */
  PolyDB(const PolyDB&);
  /** Dummy Operator to forbid the use. */
  PolyDB& operator =(const PolyDB&);
  /** The database type. */
  Type type_;
  /** The internal database. */
  FileDB* db_;
  /** The last happened error. */
  TSD<Error> error_;
  /** The custom compressor. */
  Compressor* zcomp_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
