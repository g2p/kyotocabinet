/*************************************************************************************************
 * File tree database
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


#ifndef _KCTREEDB_H                      // duplication check
#define _KCTREEDB_H

#include <kccommon.h>
#include <kcutil.h>
#include <kcdb.h>
#include <kcthread.h>
#include <kcfile.h>
#include <kccompress.h>
#include <kccompare.h>
#include <kcmap.h>
#include <kchashdb.h>

namespace kyotocabinet {                 // common namespace


/**
 * Constants for implementation.
 */
namespace {
const int32_t TDBSLOTNUM = 16;           ///< number of cache slots
const uint8_t TDBDEFAPOW = 8;            ///< default alignment power
const uint8_t TDBDEFFPOW = 10;           ///< default free block pool power
const int64_t TDBDEFBNUM = 64LL << 10;   ///< default bucket number
const int32_t TDBDEFPSIZ = 8192;         ///< default page size
const int64_t TDBDEFCCAP = 64LL << 20;   ///< default capacity size of the cache memory
const char TDBMETAKEY[] = "@";           ///< key of the record for meta data
const int64_t TDBHEADSIZ = 64;           ///< size of the header
const int64_t TDBMOFFNUMS = 8;           ///< offset of the numbers
const char TDBLNPREFIX = 'L';            ///< prefix of leaf nodes
const char TDBINPREFIX = 'I';            ///< prefix of inner nodes
const size_t TDBAVGWAY = 16;             ///< average number of ways of each node
const size_t TDBWARMRATIO = 4;           ///< ratio of the warm cache
const size_t TDBINFLRATIO = 32;          ///< ratio of flushing inner nodes
const size_t TDBDEFLINUM = 64;           ///< default number of items in each leaf node
const size_t TDBDEFIINUM = 128;          ///< default number of items in each inner node
const size_t TDBRECBUFSIZ = 64;          ///< size of the record buffer
const int64_t TDBINIDBASE = 1LL << 48;   ///< base ID number for inner nodes
const size_t TDBINLINKMIN = 8;           ///< minimum number of links in each inner node
const int32_t TDBLEVELMAX = 16;          ///< maximum level of B+ tree
}


/**
 * File tree database.
 */
class TreeDB : public FileDB {
public:
  class Cursor;
private:
  struct Record;
  struct RecordComparator;
  struct LeafNode;
  struct Link;
  struct InnerNode;
  /** An alias of array of records. */
  typedef std::vector<Record*> RecordArray;
  /** An alias of array of records. */
  typedef std::vector<Link*> LinkArray;
  /** An alias of leaf node cache. */
  typedef LinkedHashMap<int64_t, LeafNode*> LeafCache;
  /** An alias of inner node cache. */
  typedef LinkedHashMap<int64_t, InnerNode*> InnerCache;
  /** An alias of list of cursors. */
  typedef std::list<Cursor*> CursorList;
public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor : public FileDB::Cursor {
    friend class TreeDB;
  public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(TreeDB* db) : db_(db), stack_(), kbuf_(NULL), ksiz_(0), lid_(0) {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      db_->curs_.push_back(this);
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      ScopedSpinRWLock lock(&db_->mlock_, true);
      if (kbuf_ && kbuf_ != stack_) delete[] kbuf_;
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
      db_->mlock_.lock_reader();
      if (db_->omode_ == 0) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        db_->mlock_.unlock();
        return false;
      }
      if (writable && !(db_->writer_)) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "permission denied");
        db_->mlock_.unlock();
        return false;
      }
      if (!kbuf_) {
        db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
        db_->mlock_.unlock();
        return false;
      }
      char rstack[TDBRECBUFSIZ];
      size_t rsiz = sizeof(Record) + ksiz_;
      char* rbuf = rsiz > sizeof(rstack) ? new char[rsiz] : rstack;
      Record* rec = (Record*)rbuf;
      rec->ksiz = ksiz_;
      rec->vsiz = 0;
      std::memcpy(rbuf + sizeof(*rec), kbuf_, ksiz_);
      LeafNode* node = db_->load_leaf_node(lid_, false);
      bool retry = true;
      bool reorg = false;
      if (node) {
        if (writable) {
          node->lock.lock_writer();
        } else {
          node->lock.lock_reader();
        }
        if (db_->check_leaf_node_range(node, rec)) {

          //std::cout << "ADHOC:" << std::string((char*)rec + sizeof(*rec), rec->ksiz) << std::endl;

          retry = false;
          reorg = db_->accept_impl(node, rec, visitor);
        }
        node->lock.unlock();
      }
      bool err = false;
      char lstack[TDBRECBUFSIZ];
      char* lbuf = NULL;
      size_t lsiz = 0;
      Link* link = NULL;
      int64_t hist[TDBLEVELMAX];
      int32_t hnum = 0;
      if (retry) {
        lsiz = sizeof(Link) + ksiz_;
        lbuf = lsiz > sizeof(lstack) ? new char[lsiz] : lstack;
        link = (Link*)lbuf;
        link->child = 0;
        link->ksiz = ksiz_;
        std::memcpy(lbuf + sizeof(*link), kbuf_, ksiz_);
        node = db_->search_tree(link, false, hist, &hnum);
        if (node) {
          if (writable) {
            node->lock.lock_writer();
          } else {
            node->lock.lock_reader();
          }

          //std::cout << "RETRY:" << std::string((char*)rec + sizeof(*rec), rec->ksiz) << std::endl;

          reorg = db_->accept_impl(node, rec, visitor);
          node->lock.unlock();
        } else {
          err = true;
        }
      }
      if (node) {
        if (reorg && !link) {
          lsiz = sizeof(Link) + ksiz_;
          lbuf = lsiz > sizeof(lstack) ? new char[lsiz] : lstack;
          link = (Link*)lbuf;
          link->child = 0;
          link->ksiz = ksiz_;
          std::memcpy(lbuf + sizeof(*link), kbuf_, ksiz_);
          node = db_->search_tree(link, false, hist, &hnum);
        }
        if (step) {
          if (kbuf_ != stack_) delete[] kbuf_;
          kbuf_ = NULL;
          lid_ = 0;
          if (!step_impl(node, rec)) err = true;
        }
        bool flush = false;
        if (reorg && db_->mlock_.promote()) {
          if (!db_->reorganize_tree(node, hist, hnum)) err = true;
          reorg = false;
        } else if (db_->cusage_ > db_->ccap_) {
          int32_t idx = node->id % TDBSLOTNUM;
          LeafSlot* lslot = db_->lslots_ + idx;
          if (!db_->clean_leaf_cache_part(lslot)) err = true;
          if (db_->mlock_.promote()) {
            if (!db_->flush_leaf_cache_part(lslot)) err = true;
            InnerSlot* islot = db_->islots_ + idx;
            if (islot->warm->count() > lslot->warm->count() + lslot->hot->count() + 1 &&
                !db_->flush_inner_cache_part(islot)) err = true;
          } else {
            flush = true;
          }
        }
        db_->mlock_.unlock();
        if (reorg) {
          db_->mlock_.lock_writer();
          node = db_->search_tree(link, false, hist, &hnum);
          if (node) {
            if (!db_->reorganize_tree(node, hist, hnum)) err = true;
          } else {
            err = true;
          }
          db_->mlock_.unlock();
        } else if (flush) {
          int32_t idx = node->id % TDBSLOTNUM;
          LeafSlot* lslot = db_->lslots_ + idx;
          db_->mlock_.lock_writer();
          if (!db_->flush_leaf_cache_part(lslot)) err = true;
          InnerSlot* islot = db_->islots_ + idx;
          if (islot->warm->count() > lslot->warm->count() + lslot->hot->count() + 1 &&
              !db_->flush_inner_cache_part(islot)) err = true;
          db_->mlock_.unlock();
        }
      } else {
        db_->mlock_.unlock();
      }
      if (rbuf != rstack) delete[] rbuf;
      if (lbuf != lstack) delete[] lbuf;
      return !err;
    }
    /**
     * Jump the cursor to the first record.
     * @return true on success, or false on failure.
     */
    virtual bool jump() {
      ScopedSpinRWLock lock(&db_->mlock_, false);
      if (db_->omode_ == 0) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        return false;
      }
      if (kbuf_) {
        if (kbuf_ != stack_) delete[] kbuf_;
        kbuf_ = NULL;
        lid_ = 0;
      }
      int64_t id = db_->first_;
      while (id > 0) {
        LeafNode* node = db_->load_leaf_node(id, false);
        if (!node) return false;
        ScopedSpinRWLock lock(&node->lock, false);
        RecordArray& recs = node->recs;
        if (recs.size() > 0) {
          Record* rec = recs.front();
          char* dbuf = (char*)rec + sizeof(*rec);
          kbuf_ = rec->ksiz > sizeof(stack_) ? new char[rec->ksiz] : stack_;
          ksiz_ = rec->ksiz;
          std::memcpy(kbuf_, dbuf, ksiz_);
          lid_ = id;
          return true;
        } else {
          id = node->next;
        }
      }
      return true;
    }
    /**
     * Jump the cursor onto a record.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     */
    virtual bool jump(const char* kbuf, size_t ksiz) {
      ScopedSpinRWLock lock(&db_->mlock_, false);
      if (db_->omode_ == 0) {
        db_->set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        return false;
      }
      if (kbuf_) {
        if (kbuf_ != stack_) delete[] kbuf_;
        kbuf_ = NULL;
        lid_ = 0;
      }
      char lstack[TDBRECBUFSIZ];
      size_t lsiz = sizeof(Link) + ksiz;
      char* lbuf = lsiz > sizeof(lstack) ? new char[lsiz] : lstack;
      Link* link = (Link*)lbuf;
      link->child = 0;
      link->ksiz = ksiz;
      std::memcpy(lbuf + sizeof(*link), kbuf, ksiz);
      int64_t hist[TDBLEVELMAX];
      int32_t hnum = 0;
      LeafNode* node = db_->search_tree(link, true, hist, &hnum);
      if (!node) {
        if (lbuf != lstack) delete[] lbuf;
        return false;
      }
      char rstack[TDBRECBUFSIZ];
      size_t rsiz = sizeof(Record) + ksiz;
      char* rbuf = rsiz > sizeof(rstack) ? new char[rsiz] : rstack;
      Record* rec = (Record*)rbuf;
      rec->ksiz = ksiz;
      rec->vsiz = 0;
      std::memcpy(rbuf + sizeof(*rec), kbuf, ksiz);
      bool err = false;
      node->lock.lock_reader();
      const RecordArray& recs = node->recs;
      RecordArray::const_iterator ritend = node->recs.end();
      RecordArray::const_iterator rit = std::lower_bound(recs.begin(), ritend,
                                                         rec, db_->reccomp_);
      if (rit == ritend) {
        node->lock.unlock();
        int64_t id = node->next;
        while (id > 0) {
          node = db_->load_leaf_node(id, false);
          if (!node) {
            err = true;
            break;
          }
          ScopedSpinRWLock lock(&node->lock, false);
          RecordArray& recs = node->recs;
          if (recs.size() > 0) {
            rec = recs.front();
            char* dbuf = (char*)rec + sizeof(*rec);
            kbuf_ = rec->ksiz > sizeof(stack_) ? new char[rec->ksiz] : stack_;
            ksiz_ = rec->ksiz;
            std::memcpy(kbuf_, dbuf, ksiz_);
            lid_ = id;
            break;
          } else {
            id = node->next;
          }
        }
      } else {
        rec = *rit;
        char* dbuf = (char*)rec + sizeof(*rec);
        kbuf_ = rec->ksiz > sizeof(stack_) ? new char[rec->ksiz] : stack_;
        ksiz_ = rec->ksiz;
        std::memcpy(kbuf_, dbuf, ksiz_);
        lid_ = node->id;
        node->lock.unlock();
      }
      if (rbuf != rstack) delete[] rbuf;
      if (lbuf != lstack) delete[] lbuf;
      return !err;
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
      DB::Visitor visitor;
      if (!accept(&visitor, false, true)) return false;
      if (!kbuf_) {
        db_->set_error(__FILE__, __LINE__, Error::NOREC, "no record");
        return false;
      }
      return true;
    }
  private:
    /**
     * Step the cursor to the next record.
     * @param node the leaf node.
     * @param rec the record containing the key only.
     * @return true on success, or false on failure.
     */
    bool step_impl(LeafNode* node, Record* rec) {
      node->lock.lock_reader();
      const RecordArray& recs = node->recs;
      RecordArray::const_iterator ritend = node->recs.end();
      RecordArray::const_iterator rit = std::upper_bound(recs.begin(), ritend,
                                                         rec, db_->reccomp_);
      if (rit == ritend) {
        node->lock.unlock();
        int64_t id = node->next;
        while (id > 0) {
          node = db_->load_leaf_node(id, false);
          if (!node) return false;
          ScopedSpinRWLock lock(&node->lock, false);
          RecordArray& recs = node->recs;
          if (recs.size() > 0) {
            rec = recs.front();
            char* dbuf = (char*)rec + sizeof(*rec);
            kbuf_ = rec->ksiz > sizeof(stack_) ? new char[rec->ksiz] : stack_;
            ksiz_ = rec->ksiz;
            std::memcpy(kbuf_, dbuf, ksiz_);
            lid_ = id;
            return true;
          } else {
            id = node->next;
          }
        }
      } else {
        rec = *rit;
        char* dbuf = (char*)rec + sizeof(*rec);
        kbuf_ = rec->ksiz > sizeof(stack_) ? new char[rec->ksiz] : stack_;
        ksiz_ = rec->ksiz;
        std::memcpy(kbuf_, dbuf, ksiz_);
        lid_ = node->id;
        node->lock.unlock();
      }
      return true;
    }
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    TreeDB* db_;
    /** The stack buffer for the key. */
    char stack_[TDBRECBUFSIZ];
    /** The pointer to the key region. */
    char* kbuf_;
    /** The size of the key region. */
    size_t ksiz_;
    /** The last visited leaf. */
    int64_t lid_;
  };
  /**
   * Tuning Options.
   */
  enum Option {
    TSMALL = HashDB::TSMALL,             ///< use 32-bit addressing
    TLINEAR = HashDB::TLINEAR,           ///< use linear collision chaining
    TCOMPRESS = HashDB::TCOMPRESS        ///< compress each record
  };
  /**
   * Status flags.
   */
  enum Flag {
    FOPEN = HashDB::FOPEN,               ///< whether opened
    FFATAL = HashDB::FFATAL              ///< whether with fatal error
  };
  /**
   * Default constructor.
   */
  explicit TreeDB() :
    mlock_(), omode_(0), writer_(false), autotran_(false), autosync_(false),
    hdb_(), curs_(), apow_(TDBDEFAPOW), fpow_(TDBDEFFPOW), opts_(0), bnum_(TDBDEFBNUM),
    psiz_(TDBDEFPSIZ), ccap_(TDBDEFCCAP),
    root_(0), first_(0), last_(0), lcnt_(0), icnt_(0), count_(0), cusage_(0),
    lslots_(), islots_(), reccomp_(), linkcomp_(), tran_(false), trcnt_(0) {}
  /**
   * Destructor.
   * @note If the database is not closed, it is closed implicitly.
   */
  virtual ~TreeDB() {
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
    mlock_.lock_reader();
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      mlock_.unlock();
      return false;
    }
    if (writable && !writer_) {
      set_error(__FILE__, __LINE__, Error::INVALID, "permission denied");
      mlock_.unlock();
      return false;
    }
    char lstack[TDBRECBUFSIZ];
    size_t lsiz = sizeof(Link) + ksiz;
    char* lbuf = lsiz > sizeof(lstack) ? new char[lsiz] : lstack;
    Link* link = (Link*)lbuf;
    link->child = 0;
    link->ksiz = ksiz;
    std::memcpy(lbuf + sizeof(*link), kbuf, ksiz);
    int64_t hist[TDBLEVELMAX];
    int32_t hnum = 0;
    LeafNode* node = search_tree(link, true, hist, &hnum);
    if (!node) {
      if (lbuf != lstack) delete[] lbuf;
      mlock_.unlock();
      return false;
    }
    char rstack[TDBRECBUFSIZ];
    size_t rsiz = sizeof(Record) + ksiz;
    char* rbuf = rsiz > sizeof(rstack) ? new char[rsiz] : rstack;
    Record* rec = (Record*)rbuf;
    rec->ksiz = ksiz;
    rec->vsiz = 0;
    std::memcpy(rbuf + sizeof(*rec), kbuf, ksiz);
    if (writable) {
      node->lock.lock_writer();
    } else {
      node->lock.lock_reader();
    }
    bool reorg = accept_impl(node, rec, visitor);
    node->lock.unlock();
    bool flush = false;
    bool err = false;
    if (reorg && mlock_.promote()) {
      if (!reorganize_tree(node, hist, hnum)) err = true;
      reorg = false;
    } else if (cusage_ > ccap_) {
      int32_t idx = node->id % TDBSLOTNUM;
      LeafSlot* lslot = lslots_ + idx;
      if (!clean_leaf_cache_part(lslot)) err = true;
      if (mlock_.promote()) {
        if (!flush_leaf_cache_part(lslot)) err = true;
        InnerSlot* islot = islots_ + idx;
        if (islot->warm->count() > lslot->warm->count() + lslot->hot->count() + 1 &&
            !flush_inner_cache_part(islot)) err = true;
      } else {
        flush = true;
      }
    }
    mlock_.unlock();
    if (reorg) {
      mlock_.lock_writer();
      node = search_tree(link, false, hist, &hnum);
      if (node) {
        if (!reorganize_tree(node, hist, hnum)) err = true;
      } else {
        err = true;
      }
      mlock_.unlock();
    } else if (flush) {
      int32_t idx = node->id % TDBSLOTNUM;
      LeafSlot* lslot = lslots_ + idx;
      mlock_.lock_writer();
      if (!flush_leaf_cache_part(lslot)) err = true;
      InnerSlot* islot = islots_ + idx;
      if (islot->warm->count() > lslot->warm->count() + lslot->hot->count() + 1 &&
          !flush_inner_cache_part(islot)) err = true;
      mlock_.unlock();
    }
    if (rbuf != rstack) delete[] rbuf;
    if (lbuf != lstack) delete[] lbuf;
    return !err;
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
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (writable && !writer_) {
      set_error(__FILE__, __LINE__, Error::INVALID, "permission denied");
      return false;
    }
    bool err = false;
    int64_t id = first_;
    int64_t flcnt = 0;
    while (id > 0) {
      LeafNode* node = load_leaf_node(id, false);
      if (!node) return false;
      id = node->next;
      const RecordArray& recs = node->recs;
      RecordArray keys;
      keys.reserve(recs.size());
      RecordArray::const_iterator rit = recs.begin();
      RecordArray::const_iterator ritend = recs.end();
      while (rit != ritend) {
        Record* rec = *rit;
        size_t rsiz = sizeof(*rec) + rec->ksiz;
        char* dbuf = (char*)rec + sizeof(*rec);
        Record* key = (Record*)xmalloc(rsiz);
        key->ksiz = rec->ksiz;
        key->vsiz = 0;
        char* kbuf = (char*)key + sizeof(*key);
        std::memcpy(kbuf, dbuf, rec->ksiz);
        keys.push_back(key);
        rit++;
      }
      RecordArray::const_iterator kit = keys.begin();
      RecordArray::const_iterator kitend = keys.end();
      bool reorg = false;
      while (kit != kitend) {
        Record* rec = *kit;
        if (accept_impl(node, rec, visitor)) reorg = true;
        kit++;
      }
      if (reorg) {
        Record* rec = keys.front();
        char* dbuf = (char*)rec + sizeof(*rec);
        char lstack[TDBRECBUFSIZ];
        size_t lsiz = sizeof(Link) + rec->ksiz;
        char* lbuf = lsiz > sizeof(lstack) ? new char[lsiz] : lstack;
        Link* link = (Link*)lbuf;
        link->child = 0;
        link->ksiz = rec->ksiz;
        std::memcpy(lbuf + sizeof(*link), dbuf, rec->ksiz);
        int64_t hist[TDBLEVELMAX];
        int32_t hnum = 0;
        node = search_tree(link, false, hist, &hnum);
        if (node) {
          if (!reorganize_tree(node, hist, hnum)) err = true;
        } else {
          err = true;
        }
        if (lbuf != lstack) delete[] lbuf;
      }
      if (cusage_ > ccap_) {
        for (int32_t i = 0; i < TDBSLOTNUM; i++) {
          LeafSlot* lslot = lslots_ + i;
          if (!flush_leaf_cache_part(lslot)) err = true;
        }
        InnerSlot* islot = islots_ + (flcnt++) % TDBSLOTNUM;
        if (islot->warm->count() > 2 && !flush_inner_cache_part(islot)) err = true;
      }
      kit = keys.begin();
      while (kit != kitend) {
        xfree(*kit);
        kit++;
      }
    }
    return !err;
  }
  /**
   * Get the last happened error.
   * @return the last happened error.
   */
  virtual Error error() const {
    return hdb_.error();
  }
  /**
   * Set the error information.
   * @param code an error code.
   * @param message a supplement message.
   */
  virtual void set_error(Error::Code code, const char* message) {
    hdb_.set_error(code, message);
  }
  /**
   * Open a database file.
   * @param path the path of a database file.
   * @param mode the connection mode.  HashDB::OWRITER as a writer, HashDB::OREADER as a
   * reader.  The following may be added to the writer mode by bitwise-or: HashDB::OCREATE,
   * which means it creates a new database if the file does not exist, HashDB::OTRUNCATE, which
   * means it creates a new database regardless if the file exists, HashDB::OAUTOTRAN, which
   * means each updating operation is performed in implicit transaction, HashDB::OAUTOSYNC,
   * which means each updating operation is followed by implicit synchronization with the file
   * system.  The following may be added to both of the reader mode and the writer mode by
   * bitwise-or: HashDB::ONOLOCK, which means it opens the database file without file locking,
   * HashDB::TRYLOCK, which means locking is performed without blocking.
   * @return true on success, or false on failure.
   */
  virtual bool open(const std::string& path, uint32_t mode) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    writer_ = false;
    autotran_ = false;
    autosync_ = false;
    if (mode & OWRITER) {
      writer_ = true;
      if (mode & OAUTOTRAN) autotran_ = true;
      if (mode & OAUTOSYNC) autosync_ = true;
    }
    if (!hdb_.tune_type(TYPETREE)) return false;
    if (!hdb_.tune_alignment(apow_)) return false;
    if (!hdb_.tune_fbp(fpow_)) return false;
    if (!hdb_.tune_options(opts_)) return false;
    if (!hdb_.tune_buckets(bnum_)) return false;
    if (!hdb_.open(path, mode)) return false;
    if (hdb_.type() != TYPETREE) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "invalid database type");
      hdb_.close();
      return false;
    }
    root_ = 0;
    first_ = 0;
    last_ = 0;
    count_ = 0;
    create_leaf_cache();
    create_inner_cache();
    if (writer_ && hdb_.count() < 1) {
      lcnt_ = 0;
      create_leaf_node(0, 0);
      root_ = 1;
      first_ = 1;
      last_ = 1;
      lcnt_ = 1;
      icnt_ = 0;
      count_ = 0;
      if (!reccomp_.comp) reccomp_.comp = &LEXICALCOMP;
      if (!dump_meta()) {
        delete_inner_cache();
        delete_leaf_cache();
        hdb_.close();
        return false;
      }
      if (!flush_leaf_cache(true)) {
        delete_inner_cache();
        delete_leaf_cache();
        hdb_.close();
        return false;
      }
    }
    if (!load_meta()) {
      delete_inner_cache();
      delete_leaf_cache();
      hdb_.close();
      return false;
    }
    if (psiz_ < 1 || root_ < 1 || first_ < 1 || last_ < 1 ||
        lcnt_ < 1 || icnt_ < 0 || count_ < 0) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "invalid meta data");
      delete_inner_cache();
      delete_leaf_cache();
      hdb_.close();
      return false;
    }
    omode_ = mode;
    cusage_ = 0;
    tran_ = false;
    trcnt_ = 0;
    return true;
  }
  /**
   * Close the database file.
   * @return true on success, or false on failure.
   */
  virtual bool close() {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    bool err = false;
    disable_cursors();

    // hoge
    int64_t lsiz = calc_leaf_cache_size();
    int64_t isiz = calc_inner_cache_size();
    /*
    printf("[closing: %lld = %lld + %lld]\n",
           (long long)cusage_, (long long)lsiz, (long long)isiz);
    */
    if (cusage_ != lsiz + isiz) {
      printf("YABASU difference!!\n");
      err = true;
    }


    if (!flush_leaf_cache(true)) err = true;
    if (!flush_inner_cache(true)) err = true;


    // hoge
    if (cusage_ != 0 || calc_leaf_cache_count() != 0 || calc_leaf_cache_size() != 0 ||
        calc_inner_cache_count() != 0 || calc_inner_cache_size() != 0) {
      printf("YABASU not ZERO!!\n");
      err = true;
    }



    delete_inner_cache();
    delete_leaf_cache();
    if (writer_ && !dump_meta()) err = true;
    if (!hdb_.close()) err = true;
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
  virtual bool synchronize(bool hard, FileProcessor* proc) {
    mlock_.lock_reader();
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      mlock_.unlock();
      return false;
    }
    if (!writer_) {
      set_error(__FILE__, __LINE__, Error::INVALID, "permission denied");
      mlock_.unlock();
      return false;
    }
    bool err = false;
    if (!clean_leaf_cache()) err = true;
    if (!clean_inner_cache()) err = true;
    if (!clean_leaf_cache()) err = true;
    if (!mlock_.promote()) {
      mlock_.unlock();
      mlock_.lock_writer();
    }
    if (!flush_leaf_cache(true)) err = true;
    if (!flush_inner_cache(true)) err = true;
    if (!dump_meta()) err = true;
    if (!hdb_.synchronize(hard, proc)) err = true;
    mlock_.unlock();
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
        set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
        mlock_.unlock();
        return false;
      }
      if (!writer_) {
        set_error(__FILE__, __LINE__, Error::INVALID, "permission denied");
        mlock_.unlock();
        return false;
      }
      if (!tran_) break;
      mlock_.unlock();
      if (wsec > 1.0) wsec = 1.0;
      Thread::sleep(wsec);
    }
    if (!begin_transaction_impl(hard)) {
      mlock_.unlock();
      return false;
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
  virtual bool clear() {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (!writer_) {
      set_error(__FILE__, __LINE__, Error::INVALID, "permission denied");
      return false;
    }
    disable_cursors();
    flush_leaf_cache(false);
    flush_inner_cache(false);
    bool err = false;
    if (!hdb_.clear()) err = true;
    lcnt_ = 0;
    create_leaf_node(0, 0);
    root_ = 1;
    first_ = 1;
    last_ = 1;
    lcnt_ = 1;
    icnt_ = 0;
    count_ = 0;
    if (!dump_meta()) err = true;
    if (!flush_leaf_cache(true)) err = true;
    cusage_ = 0;
    return !err;
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  virtual int64_t count() {
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
  virtual int64_t size() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return -1;
    }
    return hdb_.size();
  }
  /**
   * Get the path of the database file.
   * @return the path of the database file in bytes, or an empty string on failure.
   */
  virtual std::string path() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return "";
    }
    return hdb_.path();
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  virtual bool status(std::map<std::string, std::string>* strmap) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return false;
    }
    if (!hdb_.status(strmap)) return false;
    (*strmap)["type"] = "TreeDB";
    (*strmap)["psiz"] = strprintf("%d", psiz_);
    (*strmap)["ccap"] = strprintf("%lld", (long long)ccap_);
    (*strmap)["root"] = strprintf("%lld", (long long)root_);
    (*strmap)["first"] = strprintf("%lld", (long long)first_);
    (*strmap)["last"] = strprintf("%lld", (long long)last_);
    (*strmap)["lcnt"] = strprintf("%lld", (long long)lcnt_);
    (*strmap)["icnt"] = strprintf("%lld", (long long)icnt_);
    (*strmap)["count"] = strprintf("%lld", (long long)count_);
    (*strmap)["cusage"] = strprintf("%lld", (long long)cusage_);
    if (strmap->count("cusage_lcnt") > 0)
      (*strmap)["cusage_lcnt"] = strprintf("%lld", (long long)calc_leaf_cache_count());
    if (strmap->count("cusage_lsiz") > 0)
      (*strmap)["cusage_lsiz"] = strprintf("%lld", (long long)calc_leaf_cache_size());
    if (strmap->count("cusage_icnt") > 0)
      (*strmap)["cusage_icnt"] = strprintf("%lld", (long long)calc_inner_cache_count());
    if (strmap->count("cusage_isiz") > 0)
      (*strmap)["cusage_isiz"] = strprintf("%lld", (long long)calc_inner_cache_size());
    if (strmap->count("tree_level") > 0) {
      Link link;
      link.ksiz = 0;
      int64_t hist[TDBLEVELMAX];
      int32_t hnum = 0;
      search_tree(&link, false, hist, &hnum);
      (*strmap)["tree_level"] = strprintf("%d", hnum + 1);
    }
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
   * Set the internal error reporter.
   * @param erstrm a stream object into which internal error messages are stored.
   * @param ervbs true to report all errors, or false to report fatal errors only.
   * @return true on success, or false on failure.
   */
  virtual bool tune_error_reporter(std::ostream* erstrm, bool ervbs) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    return hdb_.tune_error_reporter(erstrm, ervbs);
  }
  /**
   * Set the power of the alignment of record size.
   * @param apow the power of the alignment of record size.
   * @return true on success, or false on failure.
   */
  virtual bool tune_alignment(int8_t apow) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    apow_ = apow >= 0 ? apow : TDBDEFAPOW;
    return true;
  }
  /**
   * Set the power of the capacity of the free block pool.
   * @param fpow the power of the capacity of the free block pool.
   * @return true on success, or false on failure.
   */
  virtual bool tune_fbp(int8_t fpow) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    fpow_ = fpow >= 0 ? fpow : TDBDEFFPOW;
    return true;
  }
  /**
   * Set the optional features.
   * @param opts the optional features.
   * @return true on success, or false on failure.
   */
  virtual bool tune_options(int8_t opts) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    opts_ = opts;
    return true;
  }
  /**
   * Set the number of buckets of the hash table.
   * @param bnum the number of buckets of the hash table.
   * @return true on success, or false on failure.
   */
  virtual bool tune_buckets(int64_t bnum) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    bnum_ = bnum > 0 ? bnum : TDBDEFBNUM;
    return true;
  }
  /**
   * Set the size of each page.
   * @param psiz the size of each page.
   * @return true on success, or false on failure.
   */
  virtual bool tune_page(int32_t psiz) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    psiz_ = psiz > 0 ? psiz : TDBDEFPSIZ;
    return true;
  }
  /**
   * Set the size of the internal memory-mapped region.
   * @param msiz the size of the internal memory-mapped region.
   * @return true on success, or false on failure.
   */
  virtual bool tune_map(int64_t msiz) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    return hdb_.tune_map(msiz);
  }
  /**
   * Set the unit step number of auto defragmentation.
   * @param dfunit the unit step number of auto defragmentation.
   * @return true on success, or false on failure.
   */
  virtual bool tune_defrag(int64_t dfunit) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    return hdb_.tune_defrag(dfunit);
  }
  /**
   * Set the capacity of the total size of the page cache.
   * @param ccap the capacity of the total size of the page cache.
   * @return true on success, or false on failure.
   */
  virtual bool tune_cache(int64_t ccap) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    ccap_ = ccap > 0 ? ccap : TDBDEFCCAP;
    return true;
  }
  /**
   * Set the data compressor.
   * @param comp the data compressor object.
   * @return true on success, or false on failure.
   */
  virtual bool tune_compressor(Compressor* comp) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    return hdb_.tune_compressor(comp);
  }
  /**
   * Set the record comparator.
   * @param comp the record comparator object.
   * @return true on success, or false on failure.
   */
  virtual bool tune_comparator(Comparator* rcomp) {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ != 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "already opened");
      return false;
    }
    reccomp_.comp = rcomp;
    return true;
  }
  /**
   * Get the opaque data.
   * @return the pointer to the opaque data region, whose size is 16 bytes.
   */
  virtual char* opaque() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return NULL;
    }
    return hdb_.opaque();
  }
  /**
   * Synchronize the opaque data.
   * @return true on success, or false on failure.
   */
  virtual bool synchronize_opaque() {
    ScopedSpinRWLock lock(&mlock_, true);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return NULL;
    }
    return hdb_.synchronize_opaque();
  }
  /**
   * Perform defragmentation of the file.
   * @param step the number of steps.  If it is not more than 0, the whole region is defraged.
   * @return true on success, or false on failure.
   */
  virtual bool defrag(int64_t step) {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return NULL;
    }
    return hdb_.defrag(step);
  }
  /**
   * Get the status flags.
   * @return the status flags, or 0 on failure.
   */
  virtual uint8_t flags() {
    ScopedSpinRWLock lock(&mlock_, false);
    if (omode_ == 0) {
      set_error(__FILE__, __LINE__, Error::INVALID, "not opened");
      return 0;
    }
    return hdb_.flags();
  }
protected:
  /**
   * Set the error information.
   * @param file the file name of the epicenter.
   * @param line the line number of the epicenter.
   * @param code an error code.
   * @param message a supplement message.
   */
  virtual void set_error(const char* file, int32_t line,
                         Error::Code code, const char* message) {
    hdb_.set_error(file, line, code, message);
  }
private:
  /**
   * Record data.
   */
  struct Record {
    uint32_t ksiz;                       ///< size of the key
    uint32_t vsiz;                       ///< size of the value
  };
  /**
   * Comparator for records.
   */
  struct RecordComparator {
    Comparator* comp;                    ///< comparator
    /** constructor */
    RecordComparator() : comp(NULL) {}
    /** comparing operator */
    bool operator()(const Record* const& a, const Record* const& b) const {
      char* akbuf = (char*)a + sizeof(*a);
      char* bkbuf = (char*)b + sizeof(*b);
      return comp->compare(akbuf, a->ksiz, bkbuf, b->ksiz) < 0;
    }
  };
  /**
   * Leaf node of B+ tree.
   */
  struct LeafNode {
    SpinRWLock lock;                     ///< lock
    int64_t id;                          ///< page ID number
    RecordArray recs;                    ///< sorted array of records
    int64_t size;                        ///< total size of records
    int64_t prev;                        ///< previous leaf node
    int64_t next;                        ///< next leaf node
    bool hot;                            ///< whether in the hot cache
    bool dirty;                          ///< whether to be written back
    bool dead;                           ///< whether to be removed
  };
  /**
   * Link to a node.
   */
  struct Link {
    int64_t child;                       ///< child node
    int32_t ksiz;                        ///< size of the key
  };
  /**
   * Comparator for links.
   */
  struct LinkComparator {
    Comparator* comp;                    ///< comparator
    /** constructor */
    LinkComparator() : comp(NULL) {}
    /** comparing operator */
    bool operator()(const Link* const& a, const Link* const& b) const {
      char* akbuf = (char*)a + sizeof(*a);
      char* bkbuf = (char*)b + sizeof(*b);
      return comp->compare(akbuf, a->ksiz, bkbuf, b->ksiz) < 0;
    }
  };
  /**
   * Inner node of B+ tree.
   */
  struct InnerNode {
    SpinRWLock lock;                     ///< lock
    int64_t id;                          ///< page ID numger
    int64_t heir;                        ///< child before the first link
    LinkArray links;                     ///< sorted array of links
    int64_t size;                        ///< total size of links
    bool dirty;                          ///< whether to be written back
    bool dead;                           ///< whether to be removed
  };
  /**
   * Slot cache of inner nodes.
   */
  struct LeafSlot {
    SpinLock lock;                       ///< lock
    LeafCache* hot;                      ///< hot cache
    LeafCache* warm;                     ///< warm cache
  };
  /**
   * Slot cache of inner nodes.
   */
  struct InnerSlot {
    SpinLock lock;                       ///< lock
    InnerCache* warm;                    ///< warm cache
  };
  /**
   * Open the leaf cache.
   */
  void create_leaf_cache() {
    int64_t bnum = bnum_ / TDBSLOTNUM + 1;
    if (bnum < INT8_MAX) bnum = INT8_MAX;
    bnum = nearbyprime(bnum);
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      lslots_[i].hot = new LeafCache(bnum);
      lslots_[i].warm = new LeafCache(bnum);
    }
  }
  /**
   * Close the leaf cache.
   */
  void delete_leaf_cache() {
    for (int32_t i = TDBSLOTNUM - 1; i >= 0; i--) {
      LeafSlot* slot = lslots_ + i;
      delete slot->warm;
      delete slot->hot;
    }
  }
  /**
   * Remove all leaf nodes from the leaf cache.
   * @param save whether to save dirty nodes.
   * @return true on success, or false on failure.
   */
  bool flush_leaf_cache(bool save) {
    bool err = false;
    for (int32_t i = TDBSLOTNUM - 1; i >= 0; i--) {
      LeafSlot* slot = lslots_ + i;
      LeafCache::Iterator it = slot->warm->begin();
      LeafCache::Iterator itend = slot->warm->end();
      while (it != itend) {
        LeafNode* node = it.value();
        it++;
        if (!flush_leaf_node(node, save)) err = true;
      }
      it = slot->hot->begin();
      itend = slot->hot->end();
      while (it != itend) {
        LeafNode* node = it.value();
        it++;
        if (!flush_leaf_node(node, save)) err = true;
      }
    }
    return !err;
  }
  /**
   * Flush a part of the leaf cache.
   * @param slot a slot of leaf nodes.
   * @return true on success, or false on failure.
   */
  bool flush_leaf_cache_part(LeafSlot* slot) {

    //printf("[%d]", (int)cusage_);

    bool err = false;
    if (slot->warm->count() > 0) {
      LeafNode* node = slot->warm->first_value();
      if (!flush_leaf_node(node, true)) err = true;
    } else if (slot->hot->count() > 0) {
      LeafNode* node = slot->hot->first_value();
      if (!flush_leaf_node(node, true)) err = true;
    }
    return !err;
  }
  /**
   * Clean all of the leaf cache.
   * @return true on success, or false on failure.
   */
  bool clean_leaf_cache() {
    bool err = false;
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      LeafSlot* slot = lslots_ + i;
      ScopedSpinLock lock(&slot->lock);
      LeafCache::Iterator it = slot->warm->begin();
      LeafCache::Iterator itend = slot->warm->end();
      while (it != itend) {
        LeafNode* node = it.value();
        if (!save_leaf_node(node)) err = true;
        it++;
      }
      it = slot->hot->begin();
      itend = slot->hot->end();
      while (it != itend) {
        LeafNode* node = it.value();
        if (!save_leaf_node(node)) err = true;
        it++;
      }
    }
    return !err;
  }
  /**
   * Clean a part of the leaf cache.
   * @param slot a slot of leaf nodes.
   * @return true on success, or false on failure.
   */
  bool clean_leaf_cache_part(LeafSlot* slot) {
    bool err = false;
    ScopedSpinLock lock(&slot->lock);
    if (slot->warm->count() > 0) {
      LeafNode* node = slot->warm->first_value();
      if (!save_leaf_node(node)) err = true;
    } else if (slot->hot->count() > 0) {
      LeafNode* node = slot->hot->first_value();
      if (!save_leaf_node(node)) err = true;
    }
    return !err;
  }
  /**
   * Create a new leaf node.
   * @param prev the ID of the previous node.
   * @param next the ID of the next node.
   * @return the created leaf node.
   */
  LeafNode* create_leaf_node(int64_t prev, int64_t next) {
    LeafNode* node = new LeafNode;
    node->id = ++lcnt_;
    node->size = sizeof(int32_t) * 2;
    node->recs.reserve(TDBDEFLINUM);
    node->prev = prev;
    node->next = next;
    node->hot = false;
    node->dirty = true;
    node->dead = false;
    int32_t sidx = node->id % TDBSLOTNUM;
    LeafSlot* slot = lslots_ + sidx;
    slot->warm->set(node->id, node, LeafCache::MLAST);
    cusage_ += node->size;
    return node;
  }
  /**
   * Remove a leaf node from the cache.
   * @param save whether to save dirty node.
   * @return true on success, or false on failure.
   */
  bool flush_leaf_node(LeafNode* node, bool save) {
    bool err = false;
    if (save && !save_leaf_node(node)) err = true;
    RecordArray::const_iterator rit = node->recs.begin();
    RecordArray::const_iterator ritend = node->recs.end();
    while (rit != ritend) {
      Record* rec = *rit;
      xfree(rec);
      rit++;
    }
    int32_t sidx = node->id % TDBSLOTNUM;
    LeafSlot* slot = lslots_ + sidx;
    if (node->hot) {
      slot->hot->remove(node->id);
    } else {
      slot->warm->remove(node->id);
    }
    cusage_ -= node->size;
    delete node;
    return !err;
  }
  /**
   * Save a leaf node.
   * @param node the leaf node.
   * @return true on success, or false on failure.
   */
  bool save_leaf_node(LeafNode* node) {
    ScopedSpinRWLock lock(&node->lock, true);
    if (!node->dirty) return true;
    bool err = false;
    char hbuf[NUMBUFSIZ];
    size_t hsiz = std::sprintf(hbuf, "%c%llX", TDBLNPREFIX, (long long)node->id);
    if (node->dead) {
      if (!hdb_.remove(hbuf, hsiz)) err = true;
    } else {
      char* rbuf = new char[node->size];
      char* wp = rbuf;
      wp += writevarnum(wp, node->prev);
      wp += writevarnum(wp, node->next);
      RecordArray::const_iterator rit = node->recs.begin();
      RecordArray::const_iterator ritend = node->recs.end();
      while (rit != ritend) {
        Record* rec = *rit;
        wp += writevarnum(wp, rec->ksiz);
        wp += writevarnum(wp, rec->vsiz);
        char* dbuf = (char*)rec + sizeof(*rec);
        std::memcpy(wp, dbuf, rec->ksiz);
        wp += rec->ksiz;
        std::memcpy(wp, dbuf + rec->ksiz, rec->vsiz);
        wp += rec->vsiz;
        rit++;
      }
      if (!hdb_.set(hbuf, hsiz, rbuf, wp - rbuf)) err = true;
      delete[] rbuf;
    }
    node->dirty = false;
    return !err;
  }
  /**
   * Load a leaf node.
   * @param id the ID number of the leaf node.
   * @param prom whether to promote the warm cache.
   * @return the loaded leaf node.
   */
  LeafNode* load_leaf_node(int64_t id, bool prom) {
    int32_t sidx = id % TDBSLOTNUM;
    LeafSlot* slot = lslots_ + sidx;
    ScopedSpinLock lock(&slot->lock);
    LeafNode** np = slot->hot->get(id, LeafCache::MLAST);
    if (np) return *np;
    if (prom) {
      if (slot->hot->count() * TDBWARMRATIO > slot->warm->count() + TDBWARMRATIO) {
        slot->hot->first_value()->hot = false;
        slot->hot->migrate(slot->hot->first_key(), slot->warm, LeafCache::MLAST);
      }
      np = slot->warm->migrate(id, slot->hot, LeafCache::MLAST);
      if (np) {
        (*np)->hot = true;
        return *np;
      }
    } else {
      LeafNode** np = slot->warm->get(id, LeafCache::MLAST);
      if (np) return *np;
    }
    char hbuf[NUMBUFSIZ];
    size_t hsiz = std::sprintf(hbuf, "%c%llX", TDBLNPREFIX, (long long)id);
    class VisitorImpl : public DB::Visitor {
    public:
      VisitorImpl() : node_(NULL) {}
      LeafNode* pop() {
        return node_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp) {
        uint64_t prev;
        size_t step = readvarnum(vbuf, vsiz, &prev);
        if (step < 1) return NOP;
        vbuf += step;
        vsiz -= step;
        uint64_t next;
        step = readvarnum(vbuf, vsiz, &next);
        if (step < 1) return NOP;
        vbuf += step;
        vsiz -= step;
        LeafNode* node = new LeafNode;
        node->size = sizeof(int32_t) * 2;
        node->prev = prev;
        node->next = next;
        while (vsiz > 1) {
          uint64_t rksiz;
          step = readvarnum(vbuf, vsiz, &rksiz);
          if (step < 1) break;
          vbuf += step;
          vsiz -= step;
          uint64_t rvsiz;
          step = readvarnum(vbuf, vsiz, &rvsiz);
          if (step < 1) break;
          vbuf += step;
          vsiz -= step;
          if (vsiz < rksiz + rvsiz) break;
          size_t rsiz = sizeof(Record) + rksiz + rvsiz;
          Record* rec = (Record*)xmalloc(rsiz);
          rec->ksiz = rksiz;
          rec->vsiz = rvsiz;
          char* dbuf = (char*)rec + sizeof(*rec);
          std::memcpy(dbuf, vbuf, rksiz);
          vbuf += rksiz;
          vsiz -= rksiz;
          std::memcpy(dbuf + rksiz, vbuf, rvsiz);
          vbuf += rvsiz;
          vsiz -= rvsiz;
          node->recs.push_back(rec);
          node->size += rsiz;
        }
        if (vsiz != 0) {
          RecordArray::const_iterator rit = node->recs.begin();
          RecordArray::const_iterator ritend = node->recs.end();
          while (rit != ritend) {
            Record* rec = *rit;
            xfree(rec);
            rit++;
          }
          delete node;
          return NOP;
        }
        node_ = node;
        return NOP;
      }
      LeafNode* node_;
    } visitor;
    if (!hdb_.accept(hbuf, hsiz, &visitor, false)) return NULL;
    LeafNode* node = visitor.pop();
    if (!node) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "leaf node was not found");
      return false;
    }
    node->id = id;
    node->hot = false;
    node->dirty = false;
    node->dead = false;
    slot->warm->set(id, node, LeafCache::MLAST);
    cusage_ += node->size;
    return node;
  }
  /**
   * Check whether a record is in the range of a leaf node.
   * @param node the leaf node.
   * @param rec the record containing the key only.
   * @return true for in range, or false for out of range.
   */
  bool check_leaf_node_range(LeafNode* node, Record* rec) {
    RecordArray& recs = node->recs;
    if (recs.size() < 1) return false;
    Record* frec = recs.front();
    Record* lrec = recs.back();
    return !reccomp_(rec, frec) && !reccomp_(lrec, rec);
  }
  /**
   * Accept a visitor at a leaf node.
   * @param node the leaf node.
   * @param rec the record containing the key only.
   * @param visitor a visitor object.
   * @return true to reorganize the tree, or false if not.
   */
  bool accept_impl(LeafNode* node, Record* rec, Visitor* visitor) {
    bool reorg = false;
    RecordArray& recs = node->recs;
    RecordArray::iterator ritend = recs.end();
    RecordArray::iterator rit = std::lower_bound(recs.begin(), ritend, rec, reccomp_);
    if (rit != ritend && !reccomp_(rec, *rit)) {
      Record* rec = *rit;
      char* kbuf = (char*)rec + sizeof(*rec);
      size_t ksiz = rec->ksiz;
      size_t vsiz;
      const char* vbuf = visitor->visit_full(kbuf, ksiz, kbuf + ksiz, rec->vsiz, &vsiz);
      if (vbuf == Visitor::REMOVE) {
        size_t rsiz = sizeof(*rec) + rec->ksiz + rec->vsiz;
        count_ -= 1;
        cusage_ -= rsiz;
        node->size -= rsiz;
        node->dirty = true;
        xfree(rec);
        recs.erase(rit);
        if (recs.size() < 1) reorg = true;
      } else if (vbuf != Visitor::NOP) {
        int64_t diff = vsiz - rec->vsiz;
        cusage_ += diff;
        node->size += diff;
        node->dirty = true;
        if (vsiz > rec->vsiz) {
          *rit = (Record*)xrealloc(rec, sizeof(*rec) + rec->ksiz + vsiz);
          rec = *rit;
          kbuf = (char*)rec + sizeof(*rec);
        }
        std::memcpy(kbuf + rec->ksiz, vbuf, vsiz);
        rec->vsiz = vsiz;
        if (node->size > psiz_ && recs.size() > 1) reorg = true;
      }
    } else {
      const char* kbuf = (char*)rec + sizeof(*rec);
      size_t ksiz = rec->ksiz;
      size_t vsiz;
      const char* vbuf = visitor->visit_empty(kbuf, ksiz, &vsiz);
      if (vbuf != Visitor::NOP && vbuf != Visitor::REMOVE) {
        size_t rsiz = sizeof(*rec) + ksiz + vsiz;
        count_ += 1;
        cusage_ += rsiz;
        node->size += rsiz;
        node->dirty = true;
        rec = (Record*)xmalloc(rsiz);
        rec->ksiz = ksiz;
        rec->vsiz = vsiz;
        char* dbuf = (char*)rec + sizeof(*rec);
        std::memcpy(dbuf, kbuf, ksiz);
        std::memcpy(dbuf + ksiz, vbuf, vsiz);
        recs.insert(rit, rec);
        if (node->size > psiz_ && recs.size() > 1) reorg = true;
      }
    }
    return reorg;
  }
  /**
   * Devide a leaf node into two.
   * @param node the leaf node.
   * @return the created node, or NULL on failure.
   */
  LeafNode* divide_leaf_node(LeafNode* node) {
    LeafNode* newnode = create_leaf_node(node->id, node->next);
    if (newnode->next > 0) {
      LeafNode* nextnode = load_leaf_node(newnode->next, false);
      if (!nextnode) return NULL;
      nextnode->prev = newnode->id;
      nextnode->dirty = true;
    }
    node->next = newnode->id;
    node->dirty = true;
    RecordArray& recs = node->recs;
    RecordArray::iterator mid = recs.begin() + recs.size() / 2;
    RecordArray::iterator rit = mid;
    RecordArray::iterator ritend = recs.end();
    RecordArray& newrecs = newnode->recs;
    while (rit != ritend) {
      Record* rec = *rit;
      newrecs.push_back(rec);
      size_t rsiz = sizeof(*rec) + rec->ksiz + rec->vsiz;
      node->size -= rsiz;
      newnode->size += rsiz;
      rit++;
    }
    recs.erase(mid, ritend);
    return newnode;
  }
  /**
   * Open the inner cache.
   */
  void create_inner_cache() {
    int64_t bnum = (bnum_ / TDBAVGWAY) / TDBSLOTNUM + 1;
    if (bnum < INT8_MAX) bnum = INT8_MAX;
    bnum = nearbyprime(bnum);
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      islots_[i].warm = new InnerCache(bnum);
    }
  }
  /**
   * Close the inner cache.
   */
  void delete_inner_cache() {
    for (int32_t i = TDBSLOTNUM - 1; i >= 0; i--) {
      InnerSlot* slot = islots_ + i;
      delete slot->warm;
    }
  }
  /**
   * Remove all inner nodes from the inner cache.
   * @param save whether to save dirty nodes.
   * @return true on success, or false on failure.
   */
  bool flush_inner_cache(bool save) {
    bool err = false;
    for (int32_t i = TDBSLOTNUM - 1; i >= 0; i--) {
      InnerSlot* slot = islots_ + i;
      InnerCache::Iterator it = slot->warm->begin();
      InnerCache::Iterator itend = slot->warm->end();
      while (it != itend) {
        InnerNode* node = it.value();
        it++;
        if (!flush_inner_node(node, save)) err = true;
      }
    }
    return !err;
  }
  /**
   * Flush a part of the inner cache.
   * @param slot a slot of inner nodes.
   * @return true on success, or false on failure.
   */
  bool flush_inner_cache_part(InnerSlot* slot) {
    bool err = false;
    if (slot->warm->count() > 0) {
      InnerNode* node = slot->warm->first_value();
      if (!flush_inner_node(node, true)) err = true;
    }
    return !err;
  }
  /**
   * Clean all of the inner cache.
   * @return true on success, or false on failure.
   */
  bool clean_inner_cache() {
    bool err = false;
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      InnerSlot* slot = islots_ + i;
      ScopedSpinLock lock(&slot->lock);
      InnerCache::Iterator it = slot->warm->begin();
      InnerCache::Iterator itend = slot->warm->end();
      while (it != itend) {
        InnerNode* node = it.value();
        if (!save_inner_node(node)) err = true;
        it++;
      }
    }
    return !err;
  }
  /**
   * Create a new inner node.
   * @param heir the ID of the child before the first link.
   * @return the created inner node.
   */
  InnerNode* create_inner_node(int64_t heir) {
    InnerNode* node = new InnerNode;
    node->id = ++icnt_ + TDBINIDBASE;
    node->heir = heir;
    node->links.reserve(TDBDEFIINUM);
    node->size = sizeof(int64_t);
    node->dirty = true;
    node->dead = false;
    int32_t sidx = node->id % TDBSLOTNUM;
    InnerSlot* slot = islots_ + sidx;
    slot->warm->set(node->id, node, InnerCache::MLAST);
    cusage_ += node->size;
    return node;
  }
  /**
   * Remove an inner node from the cache.
   * @param save whether to save dirty node.
   * @return true on success, or false on failure.
   */
  bool flush_inner_node(InnerNode* node, bool save) {
    bool err = false;
    if (save && !save_inner_node(node)) err = true;
    LinkArray::const_iterator lit = node->links.begin();
    LinkArray::const_iterator litend = node->links.end();
    while (lit != litend) {
      Link* link = *lit;
      xfree(link);
      lit++;
    }
    int32_t sidx = node->id % TDBSLOTNUM;
    InnerSlot* slot = islots_ + sidx;
    slot->warm->remove(node->id);
    cusage_ -= node->size;
    delete node;
    return !err;
  }
  /**
   * Save a inner node.
   * @param node the inner node.
   * @return true on success, or false on failure.
   */
  bool save_inner_node(InnerNode* node) {
    if (!node->dirty) return true;
    bool err = false;
    char hbuf[NUMBUFSIZ];
    size_t hsiz = std::sprintf(hbuf, "%c%llX",
                               TDBINPREFIX, (long long)(node->id - TDBINIDBASE));
    if (node->dead) {
      if (!hdb_.remove(hbuf, hsiz)) err = true;
    } else {
      char* rbuf = new char[node->size];
      char* wp = rbuf;
      wp += writevarnum(wp, node->heir);
      LinkArray::const_iterator lit = node->links.begin();
      LinkArray::const_iterator litend = node->links.end();
      while (lit != litend) {
        Link* link = *lit;
        wp += writevarnum(wp, link->child);
        wp += writevarnum(wp, link->ksiz);
        char* dbuf = (char*)link + sizeof(*link);
        std::memcpy(wp, dbuf, link->ksiz);
        wp += link->ksiz;
        lit++;
      }
      if (!hdb_.set(hbuf, hsiz, rbuf, wp - rbuf)) err = true;
      delete[] rbuf;
    }
    node->dirty = false;
    return !err;
  }
  /**
   * Load an inner node.
   * @param id the ID number of the inner node.
   * @return the loaded inner node.
   */
  InnerNode* load_inner_node(int64_t id) {
    int32_t sidx = id % TDBSLOTNUM;
    InnerSlot* slot = islots_ + sidx;
    ScopedSpinLock lock(&slot->lock);
    InnerNode** np = slot->warm->get(id, InnerCache::MLAST);
    if (np) return *np;
    char hbuf[NUMBUFSIZ];
    size_t hsiz = std::sprintf(hbuf, "%c%llX", TDBINPREFIX, (long long)(id - TDBINIDBASE));
    class VisitorImpl : public DB::Visitor {
    public:
      VisitorImpl() : node_(NULL) {}
      InnerNode* pop() {
        return node_;
      }
    private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp) {
        uint64_t heir;
        size_t step = readvarnum(vbuf, vsiz, &heir);
        if (step < 1) return NOP;
        vbuf += step;
        vsiz -= step;
        InnerNode* node = new InnerNode;
        node->size = sizeof(int64_t);
        node->heir = heir;
        while (vsiz > 1) {
          uint64_t child;
          step = readvarnum(vbuf, vsiz, &child);
          if (step < 1) break;
          vbuf += step;
          vsiz -= step;
          uint64_t rksiz;
          step = readvarnum(vbuf, vsiz, &rksiz);
          if (step < 1) break;
          vbuf += step;
          vsiz -= step;
          if (vsiz < rksiz) break;
          Link* link = (Link*)xmalloc(sizeof(*link) + rksiz);
          link->child = child;
          link->ksiz = rksiz;
          char* dbuf = (char*)link + sizeof(*link);
          std::memcpy(dbuf, vbuf, rksiz);
          vbuf += rksiz;
          vsiz -= rksiz;
          node->links.push_back(link);
          node->size += sizeof(*link) + rksiz;
        }
        if (vsiz != 0) {
          LinkArray::const_iterator lit = node->links.begin();
          LinkArray::const_iterator litend = node->links.end();
          while (lit != litend) {
            Link* link = *lit;
            xfree(link);
            lit++;
          }
          delete node;
          return NOP;
        }
        node_ = node;
        return NOP;
      }
      InnerNode* node_;
    } visitor;
    if (!hdb_.accept(hbuf, hsiz, &visitor, false)) return NULL;
    InnerNode* node = visitor.pop();
    if (!node) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "inner node was not found");
      return false;
    }
    node->id = id;
    node->dirty = false;
    node->dead = false;
    slot->warm->set(id, node, InnerCache::MLAST);
    cusage_ += node->size;
    return node;
  }
  /**
   * Search the B+ tree.
   * @param link the link containing the key only.
   * @param prom whether to promote the warm cache.
   * @param hist the array of visiting history.
   * @param hnp the pointer to the variable into which the number of the history is assigned.
   * @return the corresponding leaf node, or NULL on failure.
   */
  LeafNode* search_tree(Link* link, bool prom, int64_t* hist, int32_t* hnp) {
    int64_t id = root_;
    int32_t hnum = 0;
    while (id > TDBINIDBASE) {
      InnerNode* node = load_inner_node(id);
      if (!node) return NULL;
      hist[hnum++] = id;
      const LinkArray& links = node->links;
      LinkArray::const_iterator litbeg = links.begin();
      LinkArray::const_iterator litend = links.end();
      LinkArray::const_iterator lit = std::upper_bound(litbeg, litend, link, linkcomp_);
      if (lit == litbeg) {
        id = node->heir;
      } else {
        lit--;
        Link* link = *lit;
        id = link->child;
      }
    }
    *hnp = hnum;
    return load_leaf_node(id, prom);
  }
  /**
   * Reorganize the B+ tree.
   * @param node a leaf node.
   * @param hist the array of visiting history.
   * @param hnum the number of the history.
   * @return true on success, or false on failure.
   */
  bool reorganize_tree(LeafNode* node, int64_t* hist, int32_t hnum) {
    if (node->size > psiz_ && node->recs.size() > 1) {
      LeafNode* newnode = divide_leaf_node(node);
      if (!newnode) return false;
      if (node->id == last_) last_ = newnode->id;
      int64_t heir = node->id;
      int64_t child = newnode->id;
      Record* rec = *newnode->recs.begin();
      char* dbuf = (char*)rec + sizeof(*rec);
      int32_t ksiz = rec->ksiz;
      char* kbuf = new char[ksiz];
      std::memcpy(kbuf, dbuf, ksiz);
      while (true) {
        if (hnum < 1) {
          InnerNode* inode = create_inner_node(heir);
          add_link_inner_node(inode, child, kbuf, ksiz);
          root_ = inode->id;

          // hoge
          std::string key(kbuf, ksiz);
          printf("root?: root=%llX  heir=%llX  child=%llX   %s\n",
                 (long long)root_, (long long)heir, (long long)child, key.c_str());


          delete[] kbuf;
          break;
        }
        int64_t parent = hist[--hnum];
        InnerNode* inode = load_inner_node(parent);
        if (!inode) {
          delete[] kbuf;
          return false;
        }
        add_link_inner_node(inode, child, kbuf, ksiz);
        delete[] kbuf;
        LinkArray& links = inode->links;
        if (inode->size <= psiz_ || links.size() <= TDBINLINKMIN) break;
        LinkArray::iterator litbeg = links.begin();
        LinkArray::iterator mid = litbeg + links.size() / 2;
        Link* link = *mid;
        InnerNode* newinode = create_inner_node(link->child);
        heir = inode->id;
        child = newinode->id;
        char* dbuf = (char*)link + sizeof(*link);
        ksiz = link->ksiz;
        kbuf = new char[ksiz];
        std::memcpy(kbuf, dbuf, ksiz);
        LinkArray::iterator lit = mid + 1;
        LinkArray::iterator litend = links.end();
        while (lit != litend) {
          link = *lit;
          char* dbuf = (char*)link + sizeof(*link);
          add_link_inner_node(newinode, link->child, dbuf, link->ksiz);
          lit++;
        }
        int32_t num = newinode->links.size();
        for (int32_t i = 0; i <= num; i++) {
          Link* link = links.back();
          size_t rsiz = sizeof(*link) + link->ksiz;
          cusage_ -= rsiz;
          inode->size -= rsiz;
          xfree(link);
          links.pop_back();
        }
        inode->dirty = true;
      }
    } else if (node->recs.size() < 1) {

      // hoge
      //printf("kill\n");

    }
    return true;
  }
  /**
   * Add a link to a inner node.
   * @param node the inner node.
   * @param child the ID number of the child.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   */
  void add_link_inner_node(InnerNode* node, int64_t child, const char* kbuf, size_t ksiz) {
    size_t rsiz = sizeof(Link) + ksiz;
    Link* link = (Link*)xmalloc(rsiz);
    link->child = child;
    link->ksiz = ksiz;
    char* dbuf = (char*)link + sizeof(*link);
    std::memcpy(dbuf, kbuf, ksiz);
    LinkArray& links = node->links;
    LinkArray::iterator litend = links.end();
    LinkArray::iterator lit = std::upper_bound(links.begin(), litend, link, linkcomp_);
    links.insert(lit, link);
    node->size += rsiz;
    node->dirty = true;
    cusage_ += rsiz;
  }
  /**
   * Dump the meta data into the file.
   * @return true on success, or false on failure.
   */
  bool dump_meta() {
    char head[TDBHEADSIZ];
    std::memset(head, 0, sizeof(head));
    char* wp = head;
    if (reccomp_.comp == &LEXICALCOMP) {
      *(uint8_t*)(wp++) = 0x10;
    } else if (reccomp_.comp == &DECIMALCOMP) {
      *(uint8_t*)(wp++) = 0x11;
    } else {
      *(uint8_t*)(wp++) = 0xff;
    }
    wp = head + TDBMOFFNUMS;
    uint64_t num = hton64(psiz_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    num = hton64(root_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    num = hton64(first_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    num = hton64(last_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    num = hton64(lcnt_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    num = hton64(icnt_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    num = hton64(count_);
    std::memcpy(wp, &num, sizeof(num));
    wp += sizeof(num);
    if (!hdb_.set(TDBMETAKEY, sizeof(TDBMETAKEY) - 1, head, sizeof(head))) return false;
    return true;
  }
  /**
   * Load the meta data from the file.
   * @return true on success, or false on failure.
   */
  bool load_meta() {
    char head[TDBHEADSIZ];
    int32_t hsiz = hdb_.get(TDBMETAKEY, sizeof(TDBMETAKEY) - 1, head, sizeof(head));
    if (hsiz < 0) return false;
    if (hsiz != sizeof(head)) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "invalid meta data record");
      return false;
    }
    const char* rp = head;
    if (*(uint8_t*)rp == 0x10) {
      reccomp_.comp = &LEXICALCOMP;
      linkcomp_.comp = &LEXICALCOMP;
    } else if (*(uint8_t*)rp == 0x11) {
      reccomp_.comp = &DECIMALCOMP;
      linkcomp_.comp = &DECIMALCOMP;
    } else if (*(uint8_t*)rp != 0xff || !reccomp_.comp) {
      set_error(__FILE__, __LINE__, Error::BROKEN, "comparator is invalid");
      return false;
    }
    rp = head + TDBMOFFNUMS;
    uint64_t num;
    std::memcpy(&num, rp, sizeof(num));
    psiz_ = ntoh64(num);
    rp += sizeof(num);
    std::memcpy(&num, rp, sizeof(num));
    root_ = ntoh64(num);
    rp += sizeof(num);
    std::memcpy(&num, rp, sizeof(num));
    first_ = ntoh64(num);
    rp += sizeof(num);
    std::memcpy(&num, rp, sizeof(num));
    last_ = ntoh64(num);
    rp += sizeof(num);
    std::memcpy(&num, rp, sizeof(num));
    lcnt_ = ntoh64(num);
    rp += sizeof(num);
    std::memcpy(&num, rp, sizeof(num));
    icnt_ = ntoh64(num);
    rp += sizeof(num);
    std::memcpy(&num, rp, sizeof(num));
    count_ = ntoh64(num);
    rp += sizeof(num);
    return true;
  }
  /**
   * Caluculate the total number of nodes in the leaf cache.
   * @return the total number of nodes in the leaf cache.
   */
  int64_t calc_leaf_cache_count() {
    int64_t sum = 0;
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      LeafSlot* slot = lslots_ + i;
      sum += slot->warm->count();
      sum += slot->hot->count();
    }
    return sum;
  }
  /**
   * Caluculate the amount of memory usage of the leaf cache.
   * @return the amount of memory usage of the leaf cache.
   */
  int64_t calc_leaf_cache_size() {
    int64_t sum = 0;
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      LeafSlot* slot = lslots_ + i;
      LeafCache::Iterator it = slot->warm->begin();
      LeafCache::Iterator itend = slot->warm->end();
      while (it != itend) {
        LeafNode* node = it.value();
        sum += node->size;
        it++;
      }
      it = slot->hot->begin();
      itend = slot->hot->end();
      while (it != itend) {
        LeafNode* node = it.value();
        sum += node->size;
        it++;
      }
    }
    return sum;
  }
  /**
   * Caluculate the total number of nodes in the inner cache.
   * @return the total number of nodes in the inner cache.
   */
  int64_t calc_inner_cache_count() {
    int64_t sum = 0;
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      InnerSlot* slot = islots_ + i;
      sum += slot->warm->count();
    }
    return sum;
  }
  /**
   * Caluculate the amount of memory usage of the inner cache.
   * @return the amount of memory usage of the inner cache.
   */
  int64_t calc_inner_cache_size() {
    int64_t sum = 0;
    for (int32_t i = 0; i < TDBSLOTNUM; i++) {
      InnerSlot* slot = islots_ + i;
      InnerCache::Iterator it = slot->warm->begin();
      InnerCache::Iterator itend = slot->warm->end();
      while (it != itend) {
        InnerNode* node = it.value();
        sum += node->size;
        it++;
      }
    }
    return sum;
  }
  /**
   * Disable all cursors.
   */
  void disable_cursors() {
    if (curs_.size() < 1) return;
    CursorList::const_iterator cit = curs_.begin();
    CursorList::const_iterator citend = curs_.end();
    while (cit != citend) {
      Cursor* cur = *cit;
      if (cur->kbuf_) {
        if (cur->kbuf_ != cur->stack_) delete[] cur->kbuf_;
        cur->kbuf_ = NULL;
        cur->lid_ = 0;
      }
      cit++;
    }
  }
  /**
   * Begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction_impl(bool hard) {
    if (!clean_leaf_cache()) return false;
    if (!clean_inner_cache()) return false;
    int32_t idx = trcnt_ % TDBSLOTNUM;
    LeafSlot* lslot = lslots_ + idx;
    if (lslot->warm->count() + lslot->hot->count() > 1) flush_leaf_cache_part(lslot);
    InnerSlot* islot = islots_ + idx;
    if (islot->warm->count() > 1) flush_inner_cache_part(islot);
    if (!dump_meta()) return false;
    if (!hdb_.begin_transaction(hard)) return false;
    trcnt_++;
    return true;
  }
  /**
   * Commit transaction.
   * @return true on success, or false on failure.
   */
  bool commit_transaction() {
    bool err = false;
    if (!clean_leaf_cache()) return false;
    if (!clean_inner_cache()) return false;
    if (!dump_meta()) err = true;
    if (!hdb_.end_transaction(true)) return false;
    return !err;
  }
  /**
   * Abort transaction.
   * @return true on success, or false on failure.
   */
  bool abort_transaction() {
    bool err = false;
    flush_leaf_cache(false);
    flush_inner_cache(false);
    if (!hdb_.end_transaction(false)) err = true;
    if (!load_meta()) err = true;
    disable_cursors();
    return !err;
  }



  /** Dummy constructor to forbid the use. */
  TreeDB(const TreeDB&);
  /** Dummy Operator to forbid the use. */
  TreeDB& operator =(const TreeDB&);
  /** The method lock. */
  SpinRWLock mlock_;
  /** The open mode. */
  uint32_t omode_;
  /** The flag for writer. */
  bool writer_;
  /** The flag for auto transaction. */
  bool autotran_;
  /** The flag for auto synchronization. */
  bool autosync_;
  /** The internal hash database. */
  HashDB hdb_;
  /** The cursor objects. */
  CursorList curs_;
  /** The alignment power. */
  uint8_t apow_;
  /** The free block pool power. */
  uint8_t fpow_;
  /** The options. */
  uint8_t opts_;
  /** The bucket number. */
  int64_t bnum_;
  /** The page size. */
  int32_t psiz_;
  /** The capacity of cache memory. */
  int64_t ccap_;
  /** The root node. */
  int64_t root_;
  /** The first node. */
  int64_t first_;
  /** The last node. */
  int64_t last_;
  /** The count of leaf nodes. */
  int64_t lcnt_;
  /** The count of inner nodes. */
  int64_t icnt_;
  /** The record number. */
  AtomicInt64 count_;
  /** The cache memory usage. */
  AtomicInt64 cusage_;
  /** The Slots of leaf nodes. */
  LeafSlot lslots_[TDBSLOTNUM];
  /** The Slots of inner nodes. */
  InnerSlot islots_[TDBSLOTNUM];
  /** The record comparator. */
  RecordComparator reccomp_;
  /** The link comparator. */
  LinkComparator linkcomp_;
  /** The flag whether in transaction. */
  bool tran_;
  /** The count of transaction. */
  int64_t trcnt_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
