/*************************************************************************************************
 * Cache database
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


#ifndef _KCCACHEDB_H                     // duplication check
#define _KCCACHEDB_H

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
const int32_t CDBSLOTNUM = 16;           ///< number of slot tables
const size_t CDBDEFBNUM = 1048583LL;     ///< default bucket number
const size_t CDBZMAPBNUM = 32768;        ///< mininum number of buckets to use mmap
const uint32_t CDBKSIZMAX = 0xfffff;     ///< maximum size of each key
const size_t CDBRECBUFSIZ = 48;          ///< size of the record buffer
}


/**
 * On-memory hash database with LRU deletion.
 */
class CacheDB : public FileDB {
public:
  class Cursor;
private:
  struct Record;
  struct TranLog;
  struct Slot;
  class Repeater;
  class Setter;
  class Remover;
  /** An alias of list of cursors. */
  typedef std::list<Cursor*> CursorList;
  /** An alias of list of transaction logs. */
  typedef std::list<TranLog> TranLogList;
public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor : public FileDB::Cursor {
    friend class CacheDB;
  public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(CacheDB* db) : db_(db), sidx_(-1), rec_(NULL) {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      db_->curs_.push_back(this);
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      db_->curs_.remove(this);
    }
    /**
     * Accept a visitor to the current record.
     * @param visitor a visitor object.
     * @param writable true for writable operation, or false for read-only operation.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return true on success, or false on failure.
     */
    virtual bool accept(Visitor* visitor, bool writable, bool step) {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (db_->omode_ == 0) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      if (writable && !(db_->omode_ & OWRITER)) {
        db_->set_error(Error::INVALID, "permission denied");
        return false;
      }
      if (sidx_ < 0 || !rec_) {
        db_->set_error(Error::NOREC, "no record");
        return false;
      }
      uint32_t rksiz = rec_->ksiz & CDBKSIZMAX;
      char* dbuf = (char*)rec_ + sizeof(*rec_);
      size_t vsiz;
      const char* vbuf = visitor->visit_full(dbuf, rksiz, dbuf + rksiz, rec_->vsiz, &vsiz);
      if (vbuf == Visitor::REMOVE) {
        uint64_t hash = db_->hash_record(dbuf, rksiz) / CDBSLOTNUM;
        Slot* slot = db_->slots_ + sidx_;
        Repeater repeater(Visitor::REMOVE, 0);
        db_->accept_impl(slot, hash, dbuf, rksiz, &repeater, true);
      } else if (vbuf == Visitor::NOP) {
        if (step) step_impl();
      } else {
        uint64_t hash = db_->hash_record(dbuf, rksiz) / CDBSLOTNUM;
        Slot* slot = db_->slots_ + sidx_;
        Repeater repeater(vbuf, vsiz);
        db_->accept_impl(slot, hash, dbuf, rksiz, &repeater, true);
        if (step) step_impl();
      }
      return true;
    }
    /**
     * Jump the cursor to the first record.
     * @return true on success, or false on failure.
     */
    virtual bool jump() {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (db_->omode_ == 0) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      for (int32_t i = 0; i < CDBSLOTNUM; i++) {
        Slot* slot = db_->slots_ + i;
        if (slot->first) {
          sidx_ = i;
          rec_ = slot->first;
          return true;
        }
      }
      db_->set_error(Error::NOREC, "no record");
      sidx_ = -1;
      rec_ = NULL;
      return true;
    }
    /**
     * Jump the cursor onto a record.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     */
    virtual bool jump(const char* kbuf, size_t ksiz) {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (db_->omode_ == 0) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      if (ksiz > CDBKSIZMAX) ksiz = CDBKSIZMAX;
      uint64_t hash = db_->hash_record(kbuf, ksiz);
      int32_t sidx = hash % CDBSLOTNUM;
      hash /= CDBSLOTNUM;
      Slot* slot = db_->slots_ + sidx;
      size_t bidx = hash % slot->bnum;
      Record* rec = slot->buckets[bidx];
      Record** entp = slot->buckets + bidx;
      uint32_t fhash = db_->fold_hash(hash) & ~CDBKSIZMAX;
      while (rec) {
        uint32_t rhash = rec->ksiz & ~CDBKSIZMAX;
        uint32_t rksiz = rec->ksiz & CDBKSIZMAX;
        if (fhash > rhash) {
          entp = &rec->left;
          rec = rec->left;
        } else if (fhash < rhash) {
          entp = &rec->right;
          rec = rec->right;
        } else {
          char* dbuf = (char*)rec + sizeof(*rec);
          int32_t kcmp = db_->compare_keys(kbuf, ksiz, dbuf, rksiz);
          if (kcmp < 0) {
            entp = &rec->left;
            rec = rec->left;
          } else if (kcmp > 0) {
            entp = &rec->right;
            rec = rec->right;
          } else {
            sidx_ = sidx;
            rec_ = rec;
            return true;
          }
        }
      }
      db_->set_error(Error::NOREC, "no record");
      sidx_ = -1;
      rec_ = NULL;
      return false;
    }
    /**
     * Jump the cursor to a record.
     * @note Equal to the original Cursor::jump method except that the parameter is std::string.
     */
    virtual bool jump(const std::string& key) {
      return jump(key.c_str(), key.size());
    }
    /**
     * Step the cursor to the next record.
     * @return true on success, or false on failure.
     */
    virtual bool step() {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (db_->omode_ == 0) {
        db_->set_error(Error::INVALID, "not opened");
        return false;
      }
      if (sidx_ < 0 || !rec_) {
        db_->set_error(Error::NOREC, "no record");
        return false;
      }
      bool err = false;
      if (!step_impl()) err = true;
      return !err;
    }
  private:
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /**
     * Step the cursor to the next record.
     * @return true on success, or false on failure.
     */
    bool step_impl() {
      rec_ = rec_->next;
      if (!rec_) {
        for (int32_t i = sidx_ + 1; i < CDBSLOTNUM; i++) {
          Slot* slot = db_->slots_ + i;
          if (slot->first) {
            sidx_ = i;
            rec_ = slot->first;
            return true;
          }
        }
        db_->set_error(Error::NOREC, "no record");
        sidx_ = -1;
        rec_ = NULL;
        return false;
      }
      return true;
    }
    /** The inner database. */
    CacheDB* db_;
    /** The index of the current slot. */
    int32_t sidx_;
    /** The current record. */
    Record* rec_;
  };
  /**
   * Default constructor.
   */
  CacheDB() :
    mlock_(), flock_(), error_(), omode_(0), curs_(), path_(""),
    bnum_(CDBDEFBNUM), capcnt_(-1), capsiz_(-1), slots_(), tran_(false) {}
  /**
   * Destructor.
   * @note If the database is not closed, it is closed implicitly.
   */
  virtual ~CacheDB() {
    if (omode_ != 0) close();
  }
  /**
   * Accept a visitor to a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   */
  virtual bool accept(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable) {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    if (writable && !(omode_ & OWRITER)) {
      set_error(Error::INVALID, "permission denied");
      return false;
    }
    if (ksiz > CDBKSIZMAX) ksiz = CDBKSIZMAX;
    uint64_t hash = hash_record(kbuf, ksiz);
    int32_t sidx = hash % CDBSLOTNUM;
    hash /= CDBSLOTNUM;
    Slot* slot = slots_ + sidx;
    slot->lock.lock();
    accept_impl(slot, hash, kbuf, ksiz, visitor, false);
    slot->lock.unlock();
    return true;
  }
  /**
   * Iterate to accept a visitor for each record.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   */
  virtual bool iterate(Visitor *visitor, bool writable) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    if (writable && !(omode_ & OWRITER)) {
      set_error(Error::INVALID, "permission denied");
      return false;
    }
    for (int32_t i = 0; i < CDBSLOTNUM; i++) {
      Slot* slot = slots_ + i;
      Record* rec = slot->first;
      while (rec) {
        Record* next = rec->next;
        uint32_t rksiz = rec->ksiz & CDBKSIZMAX;
        char* dbuf = (char*)rec + sizeof(*rec);
        size_t vsiz;
        const char* vbuf = visitor->visit_full(dbuf, rksiz, dbuf + rksiz, rec->vsiz, &vsiz);
        if (vbuf == Visitor::REMOVE) {
          uint64_t hash = hash_record(dbuf, rksiz) / CDBSLOTNUM;
          Repeater repeater(Visitor::REMOVE, 0);
          accept_impl(slot, hash, dbuf, rksiz, &repeater, true);
        } else if (vbuf != Visitor::NOP) {
          uint64_t hash = hash_record(dbuf, rksiz) / CDBSLOTNUM;
          Repeater repeater(vbuf, vsiz);
          accept_impl(slot, hash, dbuf, rksiz, &repeater, true);
        }
        rec = next;
      }
    }
    return true;
  }
  /**
   * Get the last happened error.
   * @return the last happened error.
   */
  virtual Error error() const {
    return error_;
  }
  /**
   * Set the error information.
   * @param code an error code.
   * @param message a supplement message.
   */
  virtual void set_error(Error::Code code, const char* message) {
    error_->set(code, message);
  }
  /**
   * Open a database file.
   * @param path the path of a database file.
   * @param mode the connection mode.  FileDB::OWRITER as a writer, FileDB::OREADER as a reader.
   * The following may be added to the writer mode by bitwise-or: FileDB::OCREATE, which means
   * it creates a new database if the file does not exist, FileDB::OTRUNCATE, which means it
   * creates a new database regardless if the file exists, FileDB::OAUTOTRAN, which means each
   * updating operation is performed in implicit transaction, FileDB::OAUTOSYNC, which means
   * each updating operation is followed by implicit synchronization with the file system.  The
   * following may be added to both of the reader mode and the writer mode by bitwise-or:
   * FileDB::ONOLOCK, which means it opens the database file without file locking,
   * FileDB::OTRYLOCK, which means locking is performed without blocking, File::ONOREPAIR, which
   * means the database file is not repaired implicitly even if file destruction is detected.
   * @return true on success, or false on failure.
   */
  virtual bool open(const std::string& path, uint32_t mode) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(Error::INVALID, "already opened");
      return false;
    }
    omode_ = mode;
    path_.append(path);
    size_t bnum = nearbyprime(bnum_ / CDBSLOTNUM);
    size_t capcnt = capcnt_ > 0 ? capcnt_ / CDBSLOTNUM + 1 : (1ULL << (sizeof(capcnt) * 8 - 1));
    size_t capsiz = capsiz_ > 0 ? capsiz_ / CDBSLOTNUM + 1 : (1ULL << (sizeof(capsiz) * 8 - 1));
    if (capsiz > sizeof(*this) / CDBSLOTNUM) capsiz -= sizeof(*this) / CDBSLOTNUM;
    if (capsiz > bnum * sizeof(Record*)) capsiz -= bnum * sizeof(Record*);
    for (int32_t i = 0; i < CDBSLOTNUM; i++) {
      initialize_slot(slots_ + i, bnum, capcnt, capsiz);
    }
    return true;
  }
  /**
   * Close the database file.
   * @return true on success, or false on failure.
   */
  virtual bool close() {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    tran_ = false;
    for (int32_t i = CDBSLOTNUM - 1; i >= 0; i--) {
      destroy_slot(slots_ + i);
    }
    path_.clear();
    omode_ = 0;
    return true;
  }
  /**
   * Synchronize updated contents with the file and the device.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @param proc a postprocessor object.  If it is NULL, no postprocessing is performed.
   * @return true on success, or false on failure.
   */
  virtual bool synchronize(bool hard, FileProcessor* proc) {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    if (!(omode_ & OWRITER)) {
      set_error(Error::INVALID, "permission denied");
      return false;
    }
    bool err = false;
    if (proc && !proc->process(path_)) {
      set_error(Error::MISC, "postprocessing failed");
      err = true;
    }
    return !err;
  }
  /**
   * Begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  virtual bool begin_transaction(bool hard) {
    for (double wsec = 1.0 / CLOCKTICK; true; wsec *= 2) {
      mlock_.lock_writer();
      if (omode_ == 0) {
        set_error(Error::INVALID, "not opened");
        mlock_.unlock();
        return false;
      }
      if (!(omode_ & OWRITER)) {
        set_error(Error::INVALID, "permission denied");
        mlock_.unlock();
        return false;
      }
      if (!tran_) break;
      mlock_.unlock();
      if (wsec > 1.0) wsec = 1.0;
      Thread::sleep(wsec);
    }
    tran_ = true;
    mlock_.unlock();
    return true;
  }
  /**
   * Commit transaction.
   * @param commit true to commit the transaction, or false to abort the transaction.
   * @return true on success, or false on failure.
   */
  virtual bool end_transaction(bool commit) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    if (!tran_) {
      set_error(Error::INVALID, "not in transaction");
      return false;
    }
    if (!commit) disable_cursors();
    for (int32_t i = 0; i < CDBSLOTNUM; i++) {
      if (!commit) apply_slot_trlogs(slots_ + i);
      slots_[i].trlogs.clear();
      adjust_slot_capacity(slots_ + i);
    }
    tran_ = false;
    return true;
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  virtual bool clear() {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    disable_cursors();
    for (int32_t i = 0; i < CDBSLOTNUM; i++) {
      Slot* slot = slots_ + i;
      clear_slot(slot);
    }
    return true;
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  virtual int64_t count() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return -1;
    }
    return count_impl();
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  virtual int64_t size() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return -1;
    }
    return size_impl();
  }
  /**
   * Get the path of the database file.
   * @return the path of the database file in bytes, or an empty string on failure.
   */
  virtual std::string path() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return "";
    }
    return path_;
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  virtual bool status(std::map<std::string, std::string>* strmap) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(Error::INVALID, "not opened");
      return false;
    }
    (*strmap)["type"] = "CacheDB";
    (*strmap)["path"] = path_;
    (*strmap)["count"] = strprintf("%lld", (long long)count_impl());
    (*strmap)["size"] = strprintf("%lld", (long long)size_impl());
    return true;
  }
  /**
   * Create a cursor object.
   * @return the return value is the cursor object.
   * @note Because the object of the return value is allocated by the constructor, it should be
   * released with the delete operator when it is no longer in use.
   */
  virtual Cursor* cursor() {
    return new Cursor(this);
  }
  /**
   * Set the number of buckets of the hash table.
   * @param bnum the number of buckets of the hash table.
   * @return true on success, or false on failure.
   */
  virtual bool tune_buckets(int64_t bnum) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(Error::INVALID, "already opened");
      return false;
    }
    bnum_ = bnum >= 0 ? bnum : CDBDEFBNUM;
    return true;
  }
  /**
   * Set the cap of record number.
   * @param count the muximum number of records.
   * @return true on success, or false on failure.
   */
  virtual bool cap_count(int64_t count) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(Error::INVALID, "already opened");
      return false;
    }
    capcnt_ = count;
    return true;
  }
  /**
   * Set the cap of memory usage.
   * @param size the muximum size of memory usage.
   * @return true on success, or false on failure.
   */
  virtual bool cap_size(int64_t size) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(Error::INVALID, "already opened");
      return false;
    }
    capsiz_ = size;
    return true;
  }
private:
  /**
   * Record data.
   */
  struct Record {
    uint32_t ksiz;                       ///< size of the key
    uint32_t vsiz;                       ///< size of the value
    Record* left;                        ///< left child record
    Record* right;                       ///< right child record
    Record* prev;                        ///< privious record
    Record* next;                        ///< next record
  };
  /**
   * Transaction log.
   */
  struct TranLog {
    bool full;                           ///< flag whether full
    std::string key;                     ///< old key
    std::string value;                   ///< old value
    /** constructor for a full record */
    TranLog(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) :
      full(true), key(kbuf, ksiz), value(vbuf, vsiz) {}
    /** constructor for an empty record */
    TranLog(const char* kbuf, size_t ksiz) : full(false), key(kbuf, ksiz) {}
  };
  /**
   * Slot table.
   */
  struct Slot {
    SpinLock lock;                       ///< lock
    Record** buckets;                    ///< bucket array
    size_t bnum;                         ///< number of buckets
    size_t capcnt;                       ///< cap of record number
    size_t capsiz;                       ///< cap of memory usage
    Record* first;                       ///< first record
    Record* last;                        ///< last record
    size_t count;                        ///< number of records
    size_t size;                         ///< total size of records
    TranLogList trlogs;                  ///< transaction logs
    size_t trsize;                       ///< size before transaction
  };
  /**
   * Repeating visitor.
   */
  class Repeater : public Visitor {
  public:
    explicit Repeater(const char* vbuf, size_t vsiz) : vbuf_(vbuf), vsiz_(vsiz) {}
  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      *sp = vsiz_;
      return vbuf_;
    }
    const char* vbuf_;
    size_t vsiz_;
  };
  /**
   * Setting visitor.
   */
  class Setter : public Visitor {
  public:
    explicit Setter(const char* vbuf, size_t vsiz) : vbuf_(vbuf), vsiz_(vsiz) {}
  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      *sp = vsiz_;
      return vbuf_;
    }
    const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp) {
      *sp = vsiz_;
      return vbuf_;
    }
    const char* vbuf_;
    size_t vsiz_;
  };
  /**
   * Removing visitor.
   */
  class Remover : public Visitor {
  private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      return REMOVE;
    }
  };
  /**
   * Accept a visitor to a record.
   * @param slot the slot of the record.
   * @param hash the hash value of the key.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param isiter true for iterator use, or false for direct use.
   */
  void accept_impl(Slot* slot, uint64_t hash, const char* kbuf, size_t ksiz, Visitor* visitor,
                   bool isiter) {
    size_t bidx = hash % slot->bnum;
    Record* rec = slot->buckets[bidx];
    Record** entp = slot->buckets + bidx;
    uint32_t fhash = fold_hash(hash) & ~CDBKSIZMAX;
    while (rec) {
      uint32_t rhash = rec->ksiz & ~CDBKSIZMAX;
      uint32_t rksiz = rec->ksiz & CDBKSIZMAX;
      if (fhash > rhash) {
        entp = &rec->left;
        rec = rec->left;
      } else if (fhash < rhash) {
        entp = &rec->right;
        rec = rec->right;
      } else {
        char* dbuf = (char*)rec + sizeof(*rec);
        int32_t kcmp = compare_keys(kbuf, ksiz, dbuf, rksiz);
        if (kcmp < 0) {
          entp = &rec->left;
          rec = rec->left;
        } else if (kcmp > 0) {
          entp = &rec->right;
          rec = rec->right;
        } else {
          size_t vsiz;
          const char* vbuf = visitor->visit_full(dbuf, rksiz, dbuf + rksiz, rec->vsiz, &vsiz);
          if (vbuf == Visitor::REMOVE) {
            if (tran_) {
              TranLog log(kbuf, ksiz, dbuf + rksiz, rec->vsiz);
              slot->trlogs.push_back(log);
            }
            if (curs_.size() > 0) escape_cursors(rec);
            if (rec == slot->first) slot->first = rec->next;
            if (rec == slot->last) slot->last = rec->prev;
            if (rec->prev) rec->prev->next = rec->next;
            if (rec->next) rec->next->prev = rec->prev;
            if (rec->left && !rec->right) {
              *entp = rec->left;
            } else if (!rec->left && rec->right) {
              *entp = rec->right;
            } else if (!rec->left) {
              *entp = NULL;
            } else {
              Record* pivot = rec->left;
              if (pivot->right) {
                Record** pentp = &pivot->right;
                pivot = pivot->right;
                while (pivot->right) {
                  pentp = &pivot->right;
                  pivot = pivot->right;
                }
                *entp = pivot;
                *pentp = pivot->left;
                pivot->left = rec->left;
                pivot->right = rec->right;
              } else {
                *entp = pivot;
                pivot->right = rec->right;
              }
            }
            slot->count--;
            slot->size -= sizeof(Record) + rksiz + rec->vsiz;
            xfree(rec);
          } else {
            bool adj = false;
            if (vbuf != Visitor::NOP) {
              if (tran_) {
                TranLog log(kbuf, ksiz, dbuf + rksiz, rec->vsiz);
                slot->trlogs.push_back(log);
              } else {
                adj = vsiz > rec->vsiz;
              }
              slot->size -= rec->vsiz;
              slot->size += vsiz;
              if (vsiz > rec->vsiz) {
                Record* old = rec;
                rec = (Record*)xrealloc(rec, sizeof(*rec) + ksiz + vsiz);
                if (rec != old) {
                  if (curs_.size() > 0) adjust_cursors(old, rec);
                  if (slot->first == old) slot->first = rec;
                  if (slot->last == old) slot->last = rec;
                  *entp = rec;
                  if (rec->prev) rec->prev->next = rec;
                  if (rec->next) rec->next->prev = rec;
                  dbuf = (char*)rec + sizeof(*rec);
                }
              }
              std::memcpy(dbuf + ksiz, vbuf, vsiz);
              rec->vsiz = vsiz;
            }
            if (!isiter && slot->last != rec) {
              if (curs_.size() > 0) escape_cursors(rec);
              if (slot->first == rec) slot->first = rec->next;
              if (rec->prev) rec->prev->next = rec->next;
              if (rec->next) rec->next->prev = rec->prev;
              rec->prev = slot->last;
              rec->next = NULL;
              slot->last->next = rec;
              slot->last = rec;
            }
            if (adj) adjust_slot_capacity(slot);
          }
          return;
        }
      }
    }
    size_t vsiz;
    const char* vbuf = visitor->visit_empty(kbuf, ksiz, &vsiz);
    if (vbuf != Visitor::NOP && vbuf != Visitor::REMOVE) {
      if (tran_) {
        TranLog log(kbuf, ksiz);
        slot->trlogs.push_back(log);
      }
      slot->size += sizeof(Record) + ksiz + vsiz;
      rec = (Record*)xmalloc(sizeof(*rec) + ksiz + vsiz);
      char* dbuf = (char*)rec + sizeof(*rec);
      std::memcpy(dbuf, kbuf, ksiz);
      rec->ksiz = ksiz | fhash;
      std::memcpy(dbuf + ksiz, vbuf, vsiz);
      rec->vsiz = vsiz;
      rec->left = NULL;
      rec->right = NULL;
      rec->prev = slot->last;
      rec->next = NULL;
      *entp = rec;
      if (!slot->first) slot->first = rec;
      if (slot->last) slot->last->next = rec;
      slot->last = rec;
      slot->count++;
      if (!tran_) adjust_slot_capacity(slot);
    }
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count_impl() {
    int64_t sum = 0;
    for (int32_t i = 0; i < CDBSLOTNUM; i++) {
      Slot* slot = slots_ + i;
      ScopedSpinLock lock(&slot->lock);
      sum += slot->count;
    }
    return sum;
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes.
   */
  int64_t size_impl() {
    int64_t sum = sizeof(*this);
    for (int32_t i = 0; i < CDBSLOTNUM; i++) {
      Slot* slot = slots_ + i;
      ScopedSpinLock lock(&slot->lock);
      sum += slot->bnum * sizeof(Record*);
      sum += slot->size;
    }
    return sum;
  }
  /**
   * Initialize a slot table.
   * @param the slot table.
   * @param bnum the number of buckets.
   * @param capcnt the cap of record number.
   * @param capsiz the cap of memory usage.
   */
  void initialize_slot(Slot* slot, size_t bnum, size_t capcnt, size_t capsiz) {
    Record** buckets;
    if (bnum >= CDBZMAPBNUM) {
      buckets = (Record**)mapalloc(sizeof(*buckets) * bnum);
    } else {
      buckets = new Record*[bnum];
      for (size_t i = 0; i < bnum; i++) {
        buckets[i] = NULL;
      }
    }
    slot->buckets = buckets;
    slot->bnum = bnum;
    slot->capcnt = capcnt;
    slot->capsiz = capsiz;
    slot->first = NULL;
    slot->last = NULL;
    slot->count = 0;
    slot->size = 0;
  }
  /**
   * Destroy a slot table.
   * @param the slot table.
   */
  void destroy_slot(Slot* slot) {
    slot->trlogs.clear();
    Record* rec = slot->last;
    while (rec) {
      Record* prev = rec->prev;
      xfree(rec);
      rec = prev;
    }
    if (slot->bnum >= CDBZMAPBNUM) {
      mapfree(slot->buckets);
    } else {
      delete[] slot->buckets;
    }
  }
  /**
   * Clear a slot table.
   * @param the slot table.
   */
  void clear_slot(Slot* slot) {
    Record* rec = slot->last;
    while (rec) {
      Record* prev = rec->prev;
      xfree(rec);
      rec = prev;
    }
    Record** buckets = slot->buckets;
    size_t bnum = slot->bnum;
    for (size_t i = 0; i < bnum; i++) {
      buckets[i] = NULL;
    }
    slot->first = NULL;
    slot->last = NULL;
    slot->count = 0;
    slot->size = 0;
  }
  /**
   * Apply transaction logs of a slot table.
   * @param the slot table.
   */
  void apply_slot_trlogs(Slot* slot) {
    const TranLogList& logs = slot->trlogs;
    TranLogList::const_iterator it = logs.end();
    TranLogList::const_iterator itbeg = logs.begin();
    while (it != itbeg) {
      it--;
      const char* kbuf = it->key.c_str();
      size_t ksiz = it->key.size();
      const char* vbuf = it->value.c_str();
      size_t vsiz = it->value.size();
      uint64_t hash = hash_record(kbuf, ksiz) / CDBSLOTNUM;
      if (it->full) {
        Setter setter(vbuf, vsiz);
        accept_impl(slot, hash, kbuf, ksiz, &setter, true);
      } else {
        Remover remover;
        accept_impl(slot, hash, kbuf, ksiz, &remover, true);
      }
    }
  }
  /**
   * Addjust a slot table to the capacity.
   * @param the slot table.
   */
  void adjust_slot_capacity(Slot* slot) {
    if ((slot->count > slot->capcnt || slot->size > slot->capsiz) && slot->first) {
      Record* rec = slot->first;
      uint32_t rksiz = rec->ksiz & CDBKSIZMAX;
      char* dbuf = (char*)rec + sizeof(*rec);
      char stack[CDBRECBUFSIZ];
      char* kbuf = rksiz > sizeof(stack) ? new char[rksiz] : stack;
      std::memcpy(kbuf, dbuf, rksiz);
      uint64_t hash = hash_record(kbuf, rksiz) / CDBSLOTNUM;
      Remover remover;
      accept_impl(slot, hash, dbuf, rksiz, &remover, true);
      if (kbuf != stack) delete[] kbuf;
    }
  }
  /**
   * Get the hash value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @return the hash value.
   */
  uint64_t hash_record(const char* kbuf, size_t ksiz) {
    return hashmurmur(kbuf, ksiz);
  }
  /**
   * Fold a hash value into a small number.
   * @param hash the hash number.
   * @return the result number.
   */
  uint32_t fold_hash(uint64_t hash) {
    return ((hash & 0xffffffff00000000ULL) >> 32) ^ ((hash & 0x0000ffffffff0000ULL) >> 16) ^
      ((hash & 0x000000000000ffffULL) << 16) ^ ((hash & 0x00000000ffff0000ULL) >> 0);
  }
  /**
   * Compare two keys in lexical order.
   * @param abuf one key.
   * @param abuf the size of the one key.
   * @param bbuf the other key.
   * @param bbuf the size of the other key.
   * @return positive if the former is big, or negative if the latter is big, or 0 if both are
   * equivalent.
   */
  int32_t compare_keys(const char* abuf, size_t asiz, const char* bbuf, size_t bsiz) {
    if (asiz != bsiz) return (int32_t)asiz - (int32_t)bsiz;
    return std::memcmp(abuf, bbuf, asiz);
  }
  /**
   * Escape cursors on a shifted or removed records.
   * @param rec the record.
   */
  void escape_cursors(Record* rec) {
    ScopedSpinLock lock(&flock_);
    if (curs_.size() < 1) return;
    CursorList::const_iterator cit = curs_.begin();
    CursorList::const_iterator citend = curs_.end();
    while (cit != citend) {
      Cursor* cur = *cit;
      if (cur->rec_ == rec) cur->step_impl();
      cit++;
    }
  }
  /**
   * Adjust cursors on re-allocated records.
   * @param orec the old address.
   * @param nrec the new address.
   */
  void adjust_cursors(Record* orec, Record* nrec) {
    ScopedSpinLock lock(&flock_);
    if (curs_.size() < 1) return;
    CursorList::const_iterator cit = curs_.begin();
    CursorList::const_iterator citend = curs_.end();
    while (cit != citend) {
      Cursor* cur = *cit;
      if (cur->rec_ == orec) cur->rec_ = nrec;
      cit++;
    }
  }
  /**
   * Disable all cursors.
   */
  void disable_cursors() {
    ScopedSpinLock lock(&flock_);
    CursorList::const_iterator cit = curs_.begin();
    CursorList::const_iterator citend = curs_.end();
    while (cit != citend) {
      Cursor* cur = *cit;
      cur->sidx_ = -1;
      cur->rec_ = NULL;
      cit++;
    }
  }
  /** Dummy constructor to forbid the use. */
  CacheDB(const CacheDB&);
  /** Dummy Operator to forbid the use. */
  CacheDB& operator =(const CacheDB&);
  /** The method lock. */
  SpinRWLock mlock_;
  /** The file lock. */
  SpinLock flock_;
  /** The last happened error. */
  TSD<Error> error_;
  /** The open mode. */
  uint32_t omode_;
  /** The cursor objects. */
  CursorList curs_;
  /** The path of the database file. */
  std::string path_;
  /** The bucket number. */
  int64_t bnum_;
  /** The cap of record number. */
  int64_t capcnt_;
  /** The cap of memory usage. */
  int64_t capsiz_;
  /** The slot tables. */
  Slot slots_[CDBSLOTNUM];
  /** The flag whether in transaction. */
  bool tran_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
