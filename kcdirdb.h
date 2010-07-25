/*************************************************************************************************
 * Directory database
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


#ifndef _KCDIRDB_H                       // duplication check
#define _KCDIRDB_H

#include <kccommon.h>
#include <kcutil.h>
#include <kcdb.h>
#include <kcthread.h>
#include <kcfile.h>
#include <kccompress.h>
#include <kccompare.h>
#include <kcmap.h>

namespace kyotocabinet {                 // common namespace


/**
 * Constants for implementation.
 */
namespace {
const char* DDBMAGICFILE = "__KCDIR__";  ///< magic file of the directory
const char* DDBCOMPFILE = "__comp__";    ///< compression file of the directory
const char* DDBATRANPREFIX = "_x";       ///< prefix of files for auto transaction
const char DDBCHKSUMSEED[] = "__kyotocabinet__";  ///< seed of the module checksum
const char DDBMAGICEOF[] = "_EOF_";      ///< magic data for the end of file
const uint8_t DDBRECMAGIC = 0xcc;        ///< magic data for record
const int32_t DDBRLOCKSLOT = 64;         ///< number of slots of the record lock
const int32_t DDBRECUNITSIZ = 32;        ///< unit size of a record
const char* DDBWALPATHEXT = "wal";       ///< extension of the WAL directory
const char* DDBTMPPATHEXT = "tmp";       ///< extension of the temporary directory
}


/**
 * Directory database.
 * @note This class is a concrete class to operate a hash database in a directory.  This class
 * can be inherited but overwriting methods is forbidden.  Before every database operation, it is
 * necessary to call the TreeDB::open method in order to open a database file and connect the
 * database object to it.  To avoid data missing or corruption, it is important to close every
 * database file by the TreeDB::close method when the database is no longer in use.  It is
 * forbidden for multible database objects in a process to open the same database at the same
 * time.
 */
class DirDB : public FileDB {
public:
  class Cursor;
private:
  struct Record;
  /** An alias of list of cursors. */
  typedef std::list<Cursor*> CursorList;
  /** An alias of vector of strings. */
  typedef std::vector<std::string> StringVector;
public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor : public FileDB::Cursor {
    friend class DirDB;
  public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(DirDB* db) : db_(db), dir_(), alive_(false), name_("") {
      _assert_(db);
      ScopedSpinRWLock lock(&db_->mlock_, true);
      db_->curs_.push_back(this);
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      _assert_(true);
      if (!db_) return;
      ScopedSpinRWLock lock(&db_->mlock_, true);
      db_->curs_.remove(this);
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
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (db_->omode_ == 0) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        return false;
      }
      if (writable && !(db_->writer_)) {
        db_->set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
        return false;
      }
      if (!alive_) {
        db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
        return false;
      }
      bool err = false;
      const std::string& rpath = db_->path_ + File::PATHCHR + name_;
      int64_t cnt = db_->count_;
      Record rec;
      if (db_->read_record(rpath, &rec)) {
        if (!db_->accept_visit_full(rec.kbuf, rec.ksiz, rec.vbuf, rec.vsiz, rec.rsiz,
                                    visitor, rpath, name_.c_str())) err = true;
        delete[] rec.rbuf;
        if (alive_ && step && db_->count_ == cnt) {
          do {
            if (!dir_.read(&name_)) {
              if (!disable()) err = true;
              break;
            }
          } while (*name_.c_str() == *DDBMAGICFILE);
        }
      } else {
        while (true) {
          if (!dir_.read(&name_)) {
            db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
            disable();
            break;
          }
          if (*name_.c_str() == *DDBMAGICFILE) continue;
          const std::string& npath = db_->path_ + File::PATHCHR + name_;
          if (!File::status(npath)) continue;
          if (db_->read_record(npath, &rec)) {
            if (!db_->accept_visit_full(rec.kbuf, rec.ksiz, rec.vbuf, rec.vsiz, rec.rsiz,
                                        visitor, npath, name_.c_str())) err = true;
            delete[] rec.rbuf;
            if (alive_ && step && db_->count_ == cnt) {
              do {
                if (!dir_.read(&name_)) {
                  if (!disable()) err = true;
                  break;
                }
              } while (*name_.c_str() == *DDBMAGICFILE);
            }
          } else {
            db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
            err = true;
          }
          break;
        }
      }
      return !err;
    }
    /**
     * Jump the cursor to the first record.
     * @return true on success, or false on failure.
     */
    bool jump() {
      _assert_(true);
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (alive_ && !disable()) return false;
      if (db_->omode_ == 0) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        return false;
      }
      if (!dir_.open(db_->path_)) {
        db_->set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
        return false;
      }
      alive_ = true;
      do {
        if (!dir_.read(&name_)) {
          db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
          disable();
          return false;
        }
      } while (*name_.c_str() == *DDBMAGICFILE);
      return true;
    }
    /**
     * Jump the cursor onto a record.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     */
    bool jump(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= MEMMAXSIZ);
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (alive_ && !disable()) return false;
      if (!dir_.open(db_->path_)) {
        db_->set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
        return false;
      }
      alive_ = true;
      while (true) {
        if (!dir_.read(&name_)) {
          db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
          disable();
          return false;
        }
        if (*name_.c_str() == *DDBMAGICFILE) continue;
        const std::string& rpath = db_->path_ + File::PATHCHR + name_;
        Record rec;
        if (db_->read_record(rpath, &rec)) {
          if (rec.ksiz == ksiz && !std::memcmp(rec.kbuf, kbuf, ksiz)) {
            delete[] rec.rbuf;
            break;
          }
          delete[] rec.rbuf;
        } else {
          db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
          disable();
          return false;
        }
      }
      return true;
    }
    /**
     * Jump the cursor to a record.
     * @note Equal to the original Cursor::jump method except that the parameter is std::string.
     */
    bool jump(const std::string& key) {
      _assert_(true);
      return jump(key.c_str(), key.size());
    }
    /**
     * Step the cursor to the next record.
     * @return true on success, or false on failure.
     */
    bool step() {
      _assert_(true);
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (db_->omode_ == 0) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        return false;
      }
      if (!alive_) {
        db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
        return false;
      }
      do {
        if (!dir_.read(&name_)) {
          db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
          disable();
          return false;
        }
      } while (*name_.c_str() == *DDBMAGICFILE);
      return true;
    }
    /**
     * Get the database object.
     * @return the database object.
     */
    DirDB* db() {
      _assert_(true);
      return db_;
    }
  private:
    /**
     * Disable the cursor.
     * @return true on success, or false on failure.
     */
    bool disable() {
      bool err = false;
      if (!dir_.close()) {
        db_->set_error(__FILE__, __LINE__, Error::SYSTEM, "closing a directory failed");
        err = true;
      }
      alive_ = false;
      return !err;
    }
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    DirDB* db_;
    /** The inner directory stream. */
    DirStream dir_;
    /** The flag if alive. */
    bool alive_;
    /** The current name. */
    std::string name_;
  };
  /**
   * Tuning Options.
   */
  enum Option {
    TCOMPRESS = 1 << 0                   ///< compress each record
  };
  /**
   * Default constructor.
   */
  explicit DirDB() :
    mlock_(), rlock_(), error_(), erstrm_(NULL), ervbs_(false),
    omode_(0), writer_(false), autotran_(false), autosync_(false), file_(), curs_(), path_(""),
    opts_(0), count_(0), size_(0), embcomp_(&ZLIBRAWCOMP), comp_(NULL), compchk_(0),
    tran_(false), trhard_(false), trcount_(0), trsize_(0), walpath_(""), tmppath_("") {
    _assert_(true);
  }
  /**
   * Destructor.
   * @note If the database is not closed, it is closed implicitly.
   */
  virtual ~DirDB() {
    _assert_(true);
    if (omode_ != 0) close();
    if (!curs_.empty()) {
      CursorList::const_iterator cit = curs_.begin();
      CursorList::const_iterator citend = curs_.end();
      while (cit != citend) {
        Cursor* cur = *cit;
        cur->db_ = NULL;
        cit++;
      }
    }
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
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      mlock_.unlock();
      return false;
    }
    if (writable && !writer_) {
      set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
      mlock_.unlock();
      return false;
    }
    char name[NUMBUFSIZ];
    size_t lidx = hashpath(kbuf, ksiz, name) % DDBRLOCKSLOT;
    if (writable) {
      rlock_.lock_writer(lidx);
    } else {
      rlock_.lock_reader(lidx);
    }
    bool err = false;
    if (!accept_impl(kbuf, ksiz, visitor, name)) err = true;
    rlock_.unlock(lidx);
    return !err;
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
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (writable && !writer_) {
      set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
      return false;
    }
    bool err = false;
    if (!iterate_impl(visitor)) err = true;
    return !err;
  }
  /**
   * Get the last happened error.
   * @return the last happened error.
   */
  Error error() const {
    _assert_(true);
    return error_;
  }
  /**
   * Set the error information.
   * @param code an error code.
   * @param message a supplement message.
   */
  void set_error(Error::Code code, const char* message) {
    _assert_(message);
    error_->set(code, message);
  }
  /**
   * Open a database file.
   * @param path the path of a database file.
   * @param mode the connection mode.  DirDB::OWRITER as a writer, DirDB::OREADER as a
   * reader.  The following may be added to the writer mode by bitwise-or: DirDB::OCREATE,
   * which means it creates a new database if the file does not exist, DirDB::OTRUNCATE, which
   * means it creates a new database regardless if the file exists, DirDB::OAUTOTRAN, which
   * means each updating operation is performed in implicit transaction, DirDB::OAUTOSYNC,
   * which means each updating operation is followed by implicit synchronization with the file
   * system.  The following may be added to both of the reader mode and the writer mode by
   * bitwise-or: DirDB::ONOLOCK, which means it opens the database file without file locking,
   * DirDB::OTRYLOCK, which means locking is performed without blocking, DirDB::ONOREPAIR,
   * which means the database file is not repaired implicitly even if file destruction is
   * detected.
   * @return true on success, or false on failure.
   * @note Every opened database must be closed by the DirDB::close method when it is no
   * longer in use.  It is not allowed for two or more database objects in the same process to
   * keep their connections to the same database file at the same time.
   */
  bool open(const std::string& path, uint32_t mode = OWRITER | OCREATE) {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    writer_ = false;
    autotran_ = false;
    autosync_ = false;
    uint32_t fmode = File::OREADER;
    if (mode & OWRITER) {
      writer_ = true;
      fmode = File::OWRITER;
      if (mode & OCREATE) fmode |= File::OCREATE;
      if (mode & OTRUNCATE) fmode |= File::OTRUNCATE;
      if (mode & OAUTOTRAN) autotran_ = true;
      if (mode & OAUTOSYNC) autosync_ = true;
    }
    if (mode & ONOLOCK) fmode |= File::ONOLOCK;
    if (mode & OTRYLOCK) fmode |= File::OTRYLOCK;
    size_t psiz = path.size();
    while (psiz > 0 && path[psiz-1] == File::PATHCHR) {
      psiz--;
    }
    const std::string& cpath = path.substr(0, psiz);
    const std::string& metapath = cpath + File::PATHCHR + DDBMAGICFILE;
    const std::string& comppath = cpath + File::PATHCHR + DDBCOMPFILE;
    const std::string& walpath = cpath + File::EXTCHR + DDBWALPATHEXT;
    const std::string& tmppath = cpath + File::EXTCHR + DDBTMPPATHEXT;
    bool hot = false;
    if (writer_ && (mode & OTRUNCATE) && File::status(metapath)) {
      if (!file_.open(metapath, fmode)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
        return false;
      }
      if (!remove_files(cpath)) {
        file_.close();
        return false;
      }
      if (File::status(walpath)) {
        remove_files(walpath);
        File::remove_directory(walpath);
      }
      if (!file_.close()) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
        return false;
      }
      const std::string& buf = format_meta(0, 0);
      if (!File::write_file(metapath, buf.c_str(), buf.size())) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "writing a file failed");
        return false;
      }
      if (File::status(comppath) && !File::remove(comppath)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
        return false;
      }
      hot = true;
    }
    File::Status sbuf;
    if (File::status(cpath, &sbuf)) {
      if (!sbuf.isdir) {
        set_error(__FILE__, __LINE__, Error::NOPERM, "invalid path (not directory)");
        return false;
      }
      if (!File::status(metapath)) {
        set_error(__FILE__, __LINE__, Error::BROKEN, "invalid meta data");
        return false;
      }
      if (!file_.open(metapath, fmode)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
        return false;
      }
    } else if (writer_ && (mode & OCREATE)) {
      hot = true;
      if (!File::make_directory(cpath)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "making a directory failed");
        return false;
      }
      if (!file_.open(metapath, fmode)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
        return false;
      }
    } else {
      set_error(__FILE__, __LINE__, Error::NOFILE, "open failed (file not found)");
      return false;
    }
    if (hot) {
      count_ = 0;
      size_ = 0;
      if (opts_ & TCOMPRESS) {
        comp_ = embcomp_;
        compchk_ = calc_comp_checksum();
        const std::string& buf = strprintf("%u\n", (unsigned)compchk_);
        if (!File::write_file(comppath, buf.c_str(), buf.size())) {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "writing a file failed");
          file_.close();
          return false;
        }
      } else {
        comp_ = NULL;
        compchk_ = 0;
      }
      if (autosync_ && !File::synchronize_whole()) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
        file_.close();
        return false;
      }
    } else {
      if (File::status(walpath, &sbuf)) {
        if (writer_) {
          file_.truncate(0);
        } else {
          File::write_file(metapath, "", 0);
          file_.refresh();
        }
        DirStream dir;
        if (dir.open(walpath)) {
          std::string name;
          while (dir.read(&name)) {
            const std::string& srcpath = walpath + File::PATHCHR + name;
            const std::string& destpath = cpath + File::PATHCHR + name;
            File::Status sbuf;
            if (File::status(srcpath, &sbuf)) {
              if (sbuf.size > 1) {
                File::rename(srcpath, destpath);
              } else {
                if (File::remove(destpath) || !File::status(destpath)) File::remove(srcpath);
              }
            }
          }
          dir.close();
          File::remove_directory(walpath);
          report(__FILE__, __LINE__, "info", "recovered by the WAL directory");
        }
      }
      if (File::status(comppath)) {
        opts_ |= TCOMPRESS;
        comp_ = embcomp_;
        compchk_ = calc_comp_checksum();
        int64_t nsiz;
        char* nbuf = File::read_file(comppath, &nsiz, NUMBUFSIZ);
        if (nbuf) {
          uint32_t chk = atoi(nbuf);
          delete[] nbuf;
          if (chk != compchk_) {
            set_error(__FILE__, __LINE__, Error::INVALID, "invalid compression checksum");
            file_.close();
            return false;
          }
        } else {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "reading a file failed");
          file_.close();
          return false;
        }
      }
      if (!load_meta()) {
        if (!calc_meta(cpath)) {
          file_.close();
          return false;
        }
        if (!writer_ && !(mode & ONOLOCK)) {
          const std::string& buf = format_meta(count_, size_);
          if (!File::write_file(metapath, buf.c_str(), buf.size())) {
            set_error(__FILE__, __LINE__, Error::SYSTEM, "writing a file failed");
            file_.close();
            return false;
          }
          if (!file_.refresh()) {
            set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
            file_.close();
            return false;
          }
        }
        report(__FILE__, __LINE__, "info", "re-calculated meta data");
      }
    }
    if (writer_ && !file_.truncate(0)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
      file_.close();
      return false;
    }
    if (File::status(walpath)) {
      remove_files(walpath);
      File::remove_directory(walpath);
    }
    if (File::status(tmppath)) {
      remove_files(tmppath);
      File::remove_directory(tmppath);
    }
    omode_ = mode;
    path_ = cpath;
    tran_ = false;
    walpath_ = walpath;
    tmppath_ = tmppath;
    return true;
  }
  /**
   * Close the database file.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    bool err = false;
    if (tran_ && !abort_transaction()) err = true;
    if (!disable_cursors()) err = true;
    if (writer_ && !dump_meta()) err = true;
    if (!file_.close()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
      err = true;
    }
    omode_ = 0;
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
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (!writer_) {
      set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
      return false;
    }
    rlock_.lock_reader_all();
    bool err = false;
    if (!synchronize_impl(hard, proc)) err = true;
    rlock_.unlock_all();
    return !err;
  }
  /**
   * Begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction(bool hard = false) {
    _assert_(true);
    for (double wsec = 1.0 / CLOCKTICK; true; wsec *= 2) {
      mlock_.lock_writer();
      if (omode_ == 0) {
        set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        mlock_.unlock();
        return false;
      }
      if (!writer_) {
        set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
        mlock_.unlock();
        return false;
      }
      if (!tran_) break;
      mlock_.unlock();
      if (wsec > 1.0) wsec = 1.0;
      Thread::sleep(wsec);
    }
    trhard_ = hard;
    if (!begin_transaction_impl()) {
      mlock_.unlock();
      return false;
    }
    tran_ = true;
    mlock_.unlock();
    return true;
  }
  /**
   * Try to begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction_try(bool hard = false) {
    _assert_(true);
    mlock_.lock_writer();
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      mlock_.unlock();
      return false;
    }
    if (!writer_) {
      set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
      mlock_.unlock();
      return false;
    }
    if (tran_) {
      set_error(__FILE__, __LINE__, Error::LOGIC, "competition avoided");
      mlock_.unlock();
      return false;
    }
    trhard_ = hard;
    if (!begin_transaction_impl()) {
      mlock_.unlock();
      return false;
    }
    tran_ = true;
    mlock_.unlock();
    return true;
  }
  /**
   * End transaction.
   * @param commit true to commit the transaction, or false to abort the transaction.
   * @return true on success, or false on failure.
   */
  bool end_transaction(bool commit = true) {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (!tran_) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not in transaction");
      return false;
    }
    bool err = false;
    if (commit) {
      if (!commit_transaction()) err = true;
    } else {
      if (!abort_transaction()) err = true;
    }
    tran_ = false;
    return !err;
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  bool clear() {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (!writer_) {
      set_error(__FILE__, __LINE__, Error::NOPERM, "permission denied");
      return false;
    }
    bool err = false;
    if (!disable_cursors()) err = true;
    if (tran_) {
      DirStream dir;
      if (dir.open(path_)) {
        std::string name;
        while (dir.read(&name)) {
          if (*name.c_str() == *DDBMAGICFILE) continue;
          const std::string& rpath = path_ + File::PATHCHR + name;
          const std::string& walpath = walpath_ + File::PATHCHR + name;
          if (File::status(walpath)) {
            if (!File::remove(rpath)) {
              set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
              err = true;
            }
          } else if (!File::rename(rpath, walpath)) {
            set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a file failed");
            err = true;
          }
        }
        if (!dir.close()) {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "closing a directory failed");
          err = true;
        }
      } else {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
        err = true;
      }
    } else {
      if (!remove_files(path_)) err = true;
    }
    count_ = 0;
    size_ = 0;
    return !err;
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count() {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return -1;
    }
    return count_;
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  int64_t size() {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return -1;
    }
    return size_impl();
  }
  /**
   * Get the path of the database file.
   * @return the path of the database file, or an empty string on failure.
   */
  std::string path() {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return "";
    }
    return path_;
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    (*strmap)["type"] = "DirDB";
    (*strmap)["realtype"] = strprintf("%u", (unsigned)TYPEDIR);
    (*strmap)["path"] = path_;
    (*strmap)["opts"] = strprintf("%u", opts_);
    (*strmap)["compchk"] = strprintf("%u", (unsigned)compchk_);
    (*strmap)["count"] = strprintf("%lld", (long long)count_);
    (*strmap)["size"] = strprintf("%lld", (long long)size_impl());
    return true;
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
   * Set the internal error reporter.
   * @param erstrm a stream object into which internal error messages are stored.
   * @param ervbs true to report all errors, or false to report fatal errors only.
   * @return true on success, or false on failure.
   */
  bool tune_error_reporter(std::ostream* erstrm, bool ervbs) {
    _assert_(erstrm);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    erstrm_ = erstrm;
    ervbs_ = ervbs;
    return true;
  }
  /**
   * Set the optional features.
   * @param opts the optional features by bitwise-or: DirDB::TCOMPRESS to compress each record.
   * @return true on success, or false on failure.
   */
  bool tune_options(int8_t opts) {
    _assert_(true);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    opts_ = opts;
    return true;
  }
  /**
   * Set the data compressor.
   * @param comp the data compressor object.
   * @return true on success, or false on failure.
   */
  bool tune_compressor(Compressor* comp) {
    _assert_(comp);
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    embcomp_ = comp;
    return true;
  }
protected:
  /**
   * Set the error information.
   * @param file the file name of the epicenter.
   * @param line the line number of the epicenter.
   * @param code an error code.
   * @param message a supplement message.
   */
  void set_error(const char* file, int32_t line,
                 Error::Code code, const char* message) {
    _assert_(file && message);
    set_error(code, message);
    if (ervbs_ || code == Error::BROKEN || code == Error::SYSTEM)
      report(file, line, "error", "%d: %s: %s", code, Error::codename(code), message);
  }
  /**
   * Report a message for debugging.
   * @param file the file name of the epicenter.
   * @param line the line number of the epicenter.
   * @param type the type string.
   * @param format the printf-like format string.
   * @param ... used according to the format string.
   */
  void report(const char* file, int32_t line, const char* type,
              const char* format, ...) {
    _assert_(file && line > 0 && type && format);
    if (!erstrm_) return;
    const std::string& path = path_.empty() ? "-" : path_;
    std::string message;
    va_list ap;
    va_start(ap, format);
    strprintf(&message, format, ap);
    va_end(ap);
    *erstrm_ << "[" << type << "]: " << path << ": " << file << ": " << line;
    *erstrm_ << ": " << message << std::endl;
  }
  /**
   * Report the content of a binary buffer for debugging.
   * @param file the file name of the epicenter.
   * @param line the line number of the epicenter.
   * @param type the type string.
   * @param name the name of the information.
   * @param buf the binary buffer.
   * @param size the size of the binary buffer
   */
  void report_binary(const char* file, int32_t line, const char* type,
                     const char* name, const char* buf, size_t size) {
    _assert_(file && line > 0 && type && name && buf && size <= MEMMAXSIZ);
    if (!erstrm_) return;
    char* hex = hexencode(buf, size);
    report(file, line, type, "%s=%s", name, hex);
    delete[] hex;
  }
private:
  /**
   * Record data.
   */
  struct Record {
    char* rbuf;                          ///< record buffer
    size_t rsiz;                         ///< record size
    const char* kbuf;                    ///< key buffer
    size_t ksiz;                         ///< key size
    const char* vbuf;                    ///< value buffer
    size_t vsiz;                         ///< value size
  };
  /**
   * Dump the meta data into the file.
   * @return true on success, or false on failure.
   */
  bool dump_meta() {
    _assert_(true);
    const std::string& buf = format_meta(count_, size_);
    if (!file_.write(0, buf.c_str(), buf.size())) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
      return false;
    }
    return true;
  }
  /**
   * Format the meta data.
   * @return the result string.
   */
  std::string format_meta(int64_t count, int64_t size) {
    return strprintf("%lld\n%lld\n%s\n", (long long)count, (long long)size, DDBMAGICEOF);
  }
  /**
   * Load the meta data from the file.
   * @return true on success, or false on failure.
   */
  bool load_meta() {
    _assert_(true);
    char buf[NUMBUFSIZ*3];
    size_t len = file_.size();
    if (len > sizeof(buf) - 1) len = sizeof(buf) - 1;
    if (!file_.read(0, buf, len)) return false;
    buf[len] = '\0';
    char* rp = buf;
    int64_t count = atoi(rp);
    char* pv = std::strchr(rp, '\n');
    if (!pv) return false;
    rp = pv + 1;
    int64_t size = atoi(rp);
    pv = std::strchr(rp, '\n');
    if (!pv) return false;
    rp = pv + 1;
    if (std::strlen(rp) < sizeof(DDBMAGICEOF) - 1 ||
        std::memcmp(rp, DDBMAGICEOF, sizeof(DDBMAGICEOF) - 1)) return false;
    count_ = count;
    size_ = size;
    return true;
  }
  /**
   * Calculate meta data.
   * @param cpath the path of the database file.
   * @return true on success, or false on failure.
   */
  bool calc_meta(const std::string& cpath) {
    _assert_(true);
    count_ = 0;
    size_ = 0;
    DirStream dir;
    if (!dir.open(cpath)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
      return false;
    }
    bool err = false;
    std::string name;
    while (dir.read(&name)) {
      if (*name.c_str() == *DDBMAGICFILE) continue;
      const std::string& rpath = cpath + File::PATHCHR + name;
      File::Status sbuf;
      if (File::status(rpath, &sbuf)) {
        count_ += 1;
        size_ += sbuf.size - 4;
      } else {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "checking the status of a file failed");
        err = true;
      }
    }
    if (!dir.close()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "closing a directory failed");
      err = true;
    }
    return !err;
  }
  /**
   * Calculate the compression checksum.
   * @return the compression checksum.
   */
  uint32_t calc_comp_checksum() {
    _assert_(true);
    size_t zsiz;
    char* zbuf = comp_->compress(DDBCHKSUMSEED, sizeof(DDBCHKSUMSEED) - 1, &zsiz);
    if (!zbuf) return 0;
    char name[NUMBUFSIZ];
    uint32_t hash = hashpath(zbuf, zsiz, name);
    delete[] zbuf;
    return hash;
  }
  /**
   * Remove inner files.
   * @param cpath the path of the database file.
   * @return true on success, or false on failure.
   */
  bool remove_files(const std::string& cpath) {
    _assert_(true);
    DirStream dir;
    if (!dir.open(cpath)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
      return false;
    }
    bool err = false;
    std::string name;
    while (dir.read(&name)) {
      if (*name.c_str() == *DDBMAGICFILE) continue;
      const std::string& rpath = cpath + File::PATHCHR + name;
      if (!File::remove(rpath)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
        err = true;
      }
    }
    if (!dir.close()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "closing a directory failed");
      err = true;
    }
    return !err;
  }
  /**
   * Read a record.
   * @param rpath the path of the record.
   * @param rec the record structure.
   * @return true on success, or false on failure.
   */
  bool read_record(const std::string& rpath, Record* rec) {
    _assert_(rec);
    int64_t rsiz;
    char* rbuf = File::read_file(rpath, &rsiz);
    if (!rbuf) return false;
    rec->rsiz = rsiz;
    if (comp_) {
      size_t zsiz;
      char* zbuf = comp_->decompress(rbuf, rsiz, &zsiz);
      if (!zbuf) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "data decompression failed");
        delete[] rbuf;
        return false;
      }
      delete[] rbuf;
      rbuf = zbuf;
      rsiz = zsiz;
    }
    const char* rp = rbuf;
    if (rsiz < 4 || *(const unsigned char*)rp != DDBRECMAGIC) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "invalid magic data of a record");
      report(__FILE__, __LINE__, "info", "rpath=%s", rpath.c_str());
      report_binary(__FILE__, __LINE__, "info", "rbuf", rbuf, rsiz);
      delete[] rbuf;
      return false;
    }
    rp++;
    uint64_t num;
    size_t step = readvarnum(rp, rsiz, &num);
    rp += step;
    rsiz -= step;
    size_t ksiz = num;
    if (rsiz < 2) {
      report(__FILE__, __LINE__, "info", "rpath=%s", rpath.c_str());
      delete[] rbuf;
      return false;
    }
    step = readvarnum(rp, rsiz, &num);
    rp += step;
    rsiz -= step;
    size_t vsiz = num;
    if (rsiz < 1 + (int64_t)ksiz + (int64_t)vsiz ||
        ((const unsigned char*)rp)[ksiz+vsiz] != DDBRECMAGIC) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "too short record");
      report(__FILE__, __LINE__, "info", "rpath=%s", rpath.c_str());
      delete[] rbuf;
      return false;
    }
    rec->rbuf = rbuf;
    rec->kbuf = rp;
    rec->ksiz = ksiz;
    rec->vbuf = rp + ksiz;
    rec->vsiz = vsiz;
    return true;
  }
  /**
   * Write a record.
   * @param rpath the path of the record.
   * @param name the file name of the record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param wsp the pointer to the variable into which the size of the written record is
   * assigned.
   * @return true on success, or false on failure.
   */
  bool write_record(const std::string& rpath, const char* name, const char* kbuf, size_t ksiz,
                    const char* vbuf, size_t vsiz, size_t* wsp) {
    _assert_(kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ && wsp);
    bool err = false;
    char* rbuf = new char[NUMBUFSIZ*2+ksiz+vsiz];
    char* wp = rbuf;
    *(wp++) = DDBRECMAGIC;
    wp += writevarnum(wp, ksiz);
    wp += writevarnum(wp, vsiz);
    std::memcpy(wp, kbuf, ksiz);
    wp += ksiz;
    std::memcpy(wp, vbuf, vsiz);
    wp += vsiz;
    *(wp++) = DDBRECMAGIC;
    size_t rsiz = wp - rbuf;
    if (comp_) {
      size_t zsiz;
      char* zbuf = comp_->compress(rbuf, rsiz, &zsiz);
      if (!zbuf) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "data compression failed");
        delete[] rbuf;
        *wsp = 0;
        return false;
      }
      delete[] rbuf;
      rbuf = zbuf;
      rsiz = zsiz;
    }
    if (autotran_ && !tran_) {
      const std::string& tpath = path_ + File::PATHCHR + DDBATRANPREFIX + name;
      if (!File::write_file(tpath, rbuf, rsiz)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "writing a file failed");
        err = true;
      }
      if (!File::rename(tpath, rpath)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a file failed");
        err = true;
        File::remove(tpath);
      }
    } else {
      if (!File::write_file(rpath, rbuf, rsiz)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "writing a file failed");
        err = true;
      }
    }
    delete[] rbuf;
    *wsp = rsiz;
    return !err;
  }
  /**
   * Disable all cursors.
   * @return true on success, or false on failure.
   */
  bool disable_cursors() {
    _assert_(true);
    if (curs_.empty()) return true;
    bool err = false;
    CursorList::const_iterator cit = curs_.begin();
    CursorList::const_iterator citend = curs_.end();
    while (cit != citend) {
      Cursor* cur = *cit;
      if (cur->alive_ && !cur->disable()) err = true;
      cit++;
    }
    return !err;
  }
  /**
   * Escape cursors on a free block.
   * @param rpath the file path of the record.
   * @param name the file name of the record.
   * @return true on success, or false on failure.
   */
  bool escape_cursors(const std::string& rpath, const char* name) {
    bool err = false;
    if (curs_.empty()) return true;
    CursorList::const_iterator cit = curs_.begin();
    CursorList::const_iterator citend = curs_.end();
    while (cit != citend) {
      Cursor* cur = *cit;
      if (cur->alive_ && cur->name_ == name) {
        do {
          if (!cur->dir_.read(&cur->name_)) {
            if (!cur->disable()) err = true;
            break;
          }
        } while (*cur->name_.c_str() == *DDBMAGICFILE);
      }
      cit++;
    }
    return !err;
  }
  /**
   * Accept a visitor to a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param name the encoded key.
   * @return true on success, or false on failure.
   */
  bool accept_impl(const char* kbuf, size_t ksiz, Visitor* visitor, const char* name) {
    _assert_(kbuf && ksiz <= MEMMAXSIZ && visitor && name);
    bool err = false;
    const std::string& rpath = path_ + File::PATHCHR + name;
    Record rec;
    if (read_record(rpath, &rec)) {
      if (rec.ksiz == ksiz || !std::memcmp(rec.kbuf, kbuf, ksiz)) {
        if (!accept_visit_full(kbuf, ksiz, rec.vbuf, rec.vsiz, rec.rsiz,
                               visitor, rpath, name)) err = true;
      } else {
        set_error(__FILE__, __LINE__, Error::LOGIC, "collision of the hash values");
        err = true;
      }
      delete[] rec.rbuf;
    } else {
      if (!accept_visit_empty(kbuf, ksiz, visitor, rpath, name)) err = true;
    }
    return !err;
  }
  /**
   * Accept the visit_full method.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param osiz the old size of the record.
   * @param visitor a visitor object.
   * @param rpath the file path of the record.
   * @param name the file name of the record.
   * @return true on success, or false on failure.
   */
  bool accept_visit_full(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
                         size_t osiz, Visitor *visitor, const std::string& rpath,
                         const char* name) {
    _assert_(kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ && visitor);
    bool err = false;
    size_t rsiz;
    const char* rbuf = visitor->visit_full(kbuf, ksiz, vbuf, vsiz, &rsiz);
    if (rbuf == Visitor::REMOVE) {
      if (tran_) {
        const std::string& walpath = walpath_ + File::PATHCHR + name;
        if (File::status(walpath)) {
          if (!File::remove(rpath)) {
            set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
            err = true;
          }
        } else if (!File::rename(rpath, walpath)) {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a file failed");
          err = true;
        }
      } else {
        if (!File::remove(rpath)) {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
          err = true;
        }
      }
      if (!escape_cursors(rpath, name)) err = true;
      count_ -= 1;
      size_ -= osiz;
      if (autosync_ && !File::synchronize_whole()) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
        err = true;
      }
    } else if (rbuf != Visitor::NOP) {
      if (tran_) {
        const std::string& walpath = walpath_ + File::PATHCHR + name;
        if (!File::status(walpath) && !File::rename(rpath, walpath)) {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a file failed");
          err = true;
        }
      }
      size_t wsiz;
      if (!write_record(rpath, name, kbuf, ksiz, rbuf, rsiz, &wsiz)) err = true;
      size_ += (int64_t)wsiz - (int64_t)osiz;
      if (autosync_ && !File::synchronize_whole()) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
        err = true;
      }
    }
    return !err;
  }
  /**
   * Accept the visit_empty method.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param rpath the file path of the record.
   * @param name the file name of the record.
   * @return true on success, or false on failure.
   */
  bool accept_visit_empty(const char* kbuf, size_t ksiz,
                          Visitor *visitor, const std::string& rpath, const char* name) {
    _assert_(kbuf && ksiz <= MEMMAXSIZ && visitor);
    bool err = false;
    size_t rsiz;
    const char* rbuf = visitor->visit_empty(kbuf, ksiz, &rsiz);
    if (rbuf != Visitor::NOP && rbuf != Visitor::REMOVE) {
      if (tran_) {
        const std::string& walpath = walpath_ + File::PATHCHR + name;
        if (!File::status(walpath) && !File::write_file(walpath, "", 0)) {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a file failed");
          err = true;
        }
      }
      size_t wsiz;
      if (!write_record(rpath, name, kbuf, ksiz, rbuf, rsiz, &wsiz)) err = true;
      count_ += 1;
      size_ += wsiz;
      if (autosync_ && !File::synchronize_whole()) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
        err = true;
      }
    }
    return !err;
  }
  /**
   * Iterate to accept a visitor for each record.
   * @param visitor a visitor object.
   * @return true on success, or false on failure.
   */
  bool iterate_impl(Visitor* visitor) {
    _assert_(visitor);
    DirStream dir;
    if (!dir.open(path_)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
      return false;
    }
    bool err = false;
    std::string name;
    while (dir.read(&name)) {
      if (*name.c_str() == *DDBMAGICFILE) continue;
      const std::string& rpath = path_ + File::PATHCHR + name;
      Record rec;
      if (read_record(rpath, &rec)) {
        if (!accept_visit_full(rec.kbuf, rec.ksiz, rec.vbuf, rec.vsiz, rec.rsiz,
                               visitor, rpath, name.c_str())) err = true;
        delete[] rec.rbuf;
      } else {
        set_error(__FILE__, __LINE__, Error::BROKEN, "missing record");
        err = true;
      }
    }
    if (!dir.close()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "closing a directory failed");
      err = true;
    }
    return !err;
  }
  /**
   * Synchronize updated contents with the file and the device.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @param proc a postprocessor object.
   * @return true on success, or false on failure.
   */
  bool synchronize_impl(bool hard, FileProcessor* proc) {
    _assert_(true);
    bool err = false;
    if (!dump_meta()) err = true;
    if (hard && !File::synchronize_whole()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
      err = true;
    }
    if (proc && !proc->process(path_, count_, size_impl())) {
      set_error(__FILE__, __LINE__, Error::LOGIC, "postprocessing failed");
      err = true;
    }
    if (!file_.truncate(0)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, file_.error());
      err = true;
    }
    return !err;
  }
  /**
   * Begin transaction.
   * @return true on success, or false on failure.
   */
  bool begin_transaction_impl() {
    _assert_(true);
    if (!File::make_directory(walpath_)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "making a directory failed");
      return false;
    }
    if (trhard_ && !File::synchronize_whole()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
      return false;
    }
    trcount_ = count_;
    trsize_ = size_;
    return true;
  }
  /**
   * Commit transaction.
   * @return true on success, or false on failure.
   */
  bool commit_transaction() {
    _assert_(true);
    bool err = false;
    if (!File::rename(walpath_, tmppath_)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a directory failed");
      err = true;
    }
    if (!remove_files(tmppath_)) err = true;
    if (!File::remove_directory(tmppath_)) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a directory failed");
      return false;
    }
    if (trhard_ && !File::synchronize_whole()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
      err = true;
    }
    return !err;
  }
  /**
   * Abort transaction.
   * @return true on success, or false on failure.
   */
  bool abort_transaction() {
    _assert_(true);
    bool err = false;
    if (!disable_cursors()) err = true;
    DirStream dir;
    if (dir.open(walpath_)) {
      std::string name;
      while (dir.read(&name)) {
        const std::string& srcpath = walpath_ + File::PATHCHR + name;
        const std::string& destpath = path_ + File::PATHCHR + name;
        File::Status sbuf;
        if (File::status(srcpath, &sbuf)) {
          if (sbuf.size > 1) {
            if (!File::rename(srcpath, destpath)) {
              set_error(__FILE__, __LINE__, Error::SYSTEM, "renaming a file failed");
              err = true;
            }
          } else {
            if (File::remove(destpath) || !File::status(destpath)) {
              if (!File::remove(srcpath)) {
                set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
                err = true;
              }
            } else {
              set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a file failed");
              err = true;
            }
          }
        } else {
          set_error(__FILE__, __LINE__, Error::SYSTEM, "checking a file failed");
          err = true;
        }
      }
      if (!dir.close()) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "closing a directory failed");
        err = true;
      }
      if (!File::remove_directory(walpath_)) {
        set_error(__FILE__, __LINE__, Error::SYSTEM, "removing a directory failed");
        err = true;
      }
    } else {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "opening a directory failed");
      err = true;
    }
    count_ = trcount_;
    size_ = trsize_;
    if (trhard_ && !File::synchronize_whole()) {
      set_error(__FILE__, __LINE__, Error::SYSTEM, "synchronizing the file system failed");
      err = true;
    }
    return !err;
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes.
   */
  int64_t size_impl() {
    return size_ + count_ * DDBRECUNITSIZ;
  }
  /** Dummy constructor to forbid the use. */
  DirDB(const DirDB&);
  /** Dummy Operator to forbid the use. */
  DirDB& operator =(const DirDB&);
  /** The method lock. */
  SpinRWLock mlock_;
  /** The record locks. */
  SlottedSpinRWLock<DDBRLOCKSLOT> rlock_;
  /** The last happened error. */
  TSD<Error> error_;
  /** The internal error reporter. */
  std::ostream* erstrm_;
  /** The flag to report all errors. */
  bool ervbs_;
  /** The open mode. */
  uint32_t omode_;
  /** The flag for writer. */
  bool writer_;
  /** The flag for auto transaction. */
  bool autotran_;
  /** The flag for auto synchronization. */
  bool autosync_;
  /** The file for meta data. */
  File file_;
  /** The cursor objects. */
  CursorList curs_;
  /** The path of the database file. */
  std::string path_;
  /** The options. */
  uint8_t opts_;
  /** The record number. */
  AtomicInt64 count_;
  /** The total size of records. */
  AtomicInt64 size_;
  /** The embedded data compressor. */
  Compressor* embcomp_;
  /** The data compressor. */
  Compressor* comp_;
  /** The compression checksum. */
  uint32_t compchk_;
  /** The flag whether in transaction. */
  bool tran_;
  /** The flag whether hard transaction. */
  bool trhard_;
  /** The old count before transaction. */
  int64_t trcount_;
  /** The old size before transaction. */
  int64_t trsize_;
  /** The WAL directory for transaction. */
  std::string walpath_;
  /** The temporary directory. */
  std::string tmppath_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
