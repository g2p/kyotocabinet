/*************************************************************************************************
 * Data mapping structures
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


#ifndef _KCMAP_H                         // duplication check
#define _KCMAP_H

#include <kccommon.h>
#include <kcutil.h>

namespace kyotocabinet {                 // common namespace


/**
 * Constants for implementation.
 */
namespace {
const size_t LHMDEFBNUM = 31;            ///< default bucket number of hash table
const size_t LHMZMAPBNUM = 32768;        ///< mininum number of buckets to use mmap
}


/**
 * Double linked hash map.
 * @param KEY the key type.
 * @param VALUE the value type.
 * @param HASH the hash functor.
 * @param EQUALTO the equality checking functor.
 */
template <class KEY, class VALUE,
          class HASH = std::hash<KEY>, class EQUALTO = std::equal_to<KEY> >
class LinkedHashMap {
private:
  struct Record;
public:
  /**
   * Iterator of records.
   */
  class Iterator {
  private:
    friend class LinkedHashMap;
  public:
    /**
     * Copy constructor.
     * @param src the source object.
     */
    Iterator(const Iterator& src) : map_(src.map_), rec_(src.rec_) {}
    /**
     * Get the key.
     */
    const KEY& key() {
      return rec_->key;
    }
    /**
     * Get the value.
     */
    VALUE& value() {
      return rec_->value;
    }
    /**
     * Assignment operator from the self type.
     * @param right the right operand.
     * @return the reference to itself.
     */
    Iterator& operator =(const Iterator& right) {
      map_ = right.map_;
      rec_ = right.rec_;
      return *this;
    }
    /**
     * Equality operator with the self type.
     * @param right the right operand.
     * @return true if the both are equal, or false if not.
     */
    bool operator==(const Iterator& right) const {
      return map_ == right.map_ && rec_ == right.rec_;
    }
    /**
     * Non-equality operator with the self type.
     * @param right the right operand.
     * @return false if the both are equal, or true if not.
     */
    bool operator!=(const Iterator& right) const {
      return map_ != right.map_ || rec_ != right.rec_;
    }
    /**
     * Preposting increment operator.
     * @return the iterator itself.
     */
    Iterator& operator++() {
      rec_ = rec_->next;
      return *this;
    }
    /**
     * Postpositive increment operator.
     * @return an iterator of the old position.
     */
    Iterator operator++(int) {
      Iterator old(*this);
      rec_ = rec_->next;
      return old;
    }
    /**
     * Preposting decrement operator.
     * @return the iterator itself.
     */
    Iterator& operator--() {
      if (rec_) {
        rec_ = rec_->prev;
      } else {
        rec_ = map_->last_;
      }
      return *this;
    }
    /**
     * Postpositive decrement operator.
     * @return an iterator of the old position.
     */
    Iterator operator--(int) {
      Iterator old(*this);
      if (rec_) {
        rec_ = rec_->prev;
      } else {
        rec_ = map_->last_;
      }
      return old;
    }
  private:
    /**
     * Constructor.
     * @param map the container.
     * @param rec the pointer to the current record.
     */
    explicit Iterator(LinkedHashMap* map, Record* rec) : map_(map), rec_(rec) {}
    /** The container. */
    LinkedHashMap* map_;
    /** The current record. */
    Record* rec_;
  };
  /**
   * Moving Modes.
   */
  enum MoveMode {
    MCURRENT,                            ///< keep the current position
    MFIRST,                              ///< move to the first
    MLAST                                ///< move to the last
  };
  /**
   * Default constructor.
   */
  explicit LinkedHashMap() :
    buckets_(NULL), bnum_(LHMDEFBNUM), first_(NULL), last_(NULL), count_(0) {
    initialize();
  }
  /**
   * Constructor.
   * @param bnum the number of buckets of the hash table.
   */
  explicit LinkedHashMap(size_t bnum) :
    buckets_(NULL), bnum_(bnum), first_(NULL), last_(NULL), count_(0) {
    if (bnum_ < 1) bnum_ = LHMDEFBNUM;
    initialize();
  }
  /**
   * Destructor.
   */
  ~LinkedHashMap() {
    destroy();
  }
  /**
   * Store a record.
   * @param key the key.
   * @param value the value.
   * @param mode the moving mode.
   * @return the pointer to the value of the stored record.
   */
  VALUE *set(const KEY& key, const VALUE& value, MoveMode mode) {
    size_t bidx = hash_(key) % bnum_;
    Record* rec = buckets_[bidx];
    Record** entp = buckets_ + bidx;
    while (rec) {
      if (equalto_(rec->key, key)) {
        rec->value = value;
        switch (mode) {
          default: {
            break;
          }
          case MFIRST: {
            if (first_ != rec) {
              if (last_ == rec) last_ = rec->prev;
              if (rec->prev) rec->prev->next = rec->next;
              if (rec->next) rec->next->prev = rec->prev;
              rec->prev = NULL;
              rec->next = first_;
              first_->prev = rec;
              first_ = rec;
            }
            break;
          }
          case MLAST: {
            if (last_ != rec) {
              if (first_ == rec) first_ = rec->next;
              if (rec->prev) rec->prev->next = rec->next;
              if (rec->next) rec->next->prev = rec->prev;
              rec->prev = last_;
              rec->next = NULL;
              last_->next = rec;
              last_ = rec;
            }
            break;
          }
        }
        return &rec->value;
      } else {
        entp = &rec->child;
        rec = rec->child;
      }
    }
    rec = new Record(key, value);
    switch (mode) {
      default: {
        rec->prev = last_;
        if (!first_) first_ = rec;
        if (last_) last_->next = rec;
        last_ = rec;
        break;
      }
      case MFIRST: {
        rec->next = first_;
        if (!last_) last_ = rec;
        if (first_) first_->prev = rec;
        first_ = rec;
        break;
      }
    }
    *entp = rec;
    count_++;
    return &rec->value;
  }
  /**
   * Remove a record.
   * @param key the key.
   * @return true on success, or false on failure.
   */
  bool remove(const KEY& key) {
    size_t bidx = hash_(key) % bnum_;
    Record* rec = buckets_[bidx];
    Record** entp = buckets_ + bidx;
    while (rec) {
      if (equalto_(rec->key, key)) {
        if (rec->prev) rec->prev->next = rec->next;
        if (rec->next) rec->next->prev = rec->prev;
        if (rec == first_) first_ = rec->next;
        if (rec == last_) last_ = rec->prev;
        *entp = rec->child;
        count_--;
        delete rec;
        return true;
      } else {
        entp = &rec->child;
        rec = rec->child;
      }
    }
    return false;
  }
  /**
   * Migrate a record to another map.
   * @param key the key.
   * @param dist the destination map.
   * @param mode the moving mode.
   * @return the pointer to the value of the migrated record, or NULL on failure.
   */
  VALUE* migrate(const KEY& key, LinkedHashMap* dist, MoveMode mode) {
    size_t hash = hash_(key);
    size_t bidx = hash % bnum_;
    Record* rec = buckets_[bidx];
    Record** entp = buckets_ + bidx;
    while (rec) {
      if (equalto_(rec->key, key)) {
        if (rec->prev) rec->prev->next = rec->next;
        if (rec->next) rec->next->prev = rec->prev;
        if (rec == first_) first_ = rec->next;
        if (rec == last_) last_ = rec->prev;
        *entp = rec->child;
        count_--;
        rec->child = NULL;
        rec->prev = NULL;
        rec->next = NULL;
        bidx = hash % dist->bnum_;
        Record* drec = dist->buckets_[bidx];
        entp = dist->buckets_ + bidx;
        while (drec) {
          if (dist->equalto_(drec->key, key)) {
            if (drec->child) rec->child = drec->child;
            if (drec->prev) {
              rec->prev = drec->prev;
              rec->prev->next = rec;
            }
            if (drec->next) {
              rec->next = drec->next;
              rec->next->prev = rec;
            }
            if (dist->first_ == drec) dist->first_ = rec;
            if (dist->last_ == drec) dist->last_ = rec;
            *entp = rec;
            delete drec;
            switch (mode) {
              default: {
                break;
              }
              case MFIRST: {
                if (dist->first_ != rec) {
                  if (dist->last_ == rec) dist->last_ = rec->prev;
                  if (rec->prev) rec->prev->next = rec->next;
                  if (rec->next) rec->next->prev = rec->prev;
                  rec->prev = NULL;
                  rec->next = dist->first_;
                  dist->first_->prev = rec;
                  dist->first_ = rec;
                }
                break;
              }
              case MLAST: {
                if (dist->last_ != rec) {
                  if (dist->first_ == rec) dist->first_ = rec->next;
                  if (rec->prev) rec->prev->next = rec->next;
                  if (rec->next) rec->next->prev = rec->prev;
                  rec->prev = dist->last_;
                  rec->next = NULL;
                  dist->last_->next = rec;
                  dist->last_ = rec;
                }
                break;
              }
            }
            return &rec->value;
          } else {
            entp = &drec->child;
            drec = drec->child;
          }
        }
        switch (mode) {
          default: {
            rec->prev = dist->last_;
            if (!dist->first_) dist->first_ = rec;
            if (dist->last_) dist->last_->next = rec;
            dist->last_ = rec;
            break;
          }
          case MFIRST: {
            rec->next = dist->first_;
            if (!dist->last_) dist->last_ = rec;
            if (dist->first_) dist->first_->prev = rec;
            dist->first_ = rec;
            break;
          }
        }
        *entp = rec;
        dist->count_++;
        return &rec->value;
      } else {
        entp = &rec->child;
        rec = rec->child;
      }
    }
    return NULL;
  }
  /**
   * Retrieve a record.
   * @param key the key.
   * @param mode the moving mode.
   * @return the pointer to the value of the corresponding record, or NULL on failure.
   */
  VALUE* get(const KEY& key, MoveMode mode) {
    size_t bidx = hash_(key) % bnum_;
    Record* rec = buckets_[bidx];
    while (rec) {
      if (equalto_(rec->key, key)) {
        switch (mode) {
          default: {
            break;
          }
          case MFIRST: {
            if (first_ != rec) {
              if (last_ == rec) last_ = rec->prev;
              if (rec->prev) rec->prev->next = rec->next;
              if (rec->next) rec->next->prev = rec->prev;
              rec->prev = NULL;
              rec->next = first_;
              first_->prev = rec;
              first_ = rec;
            }
            break;
          }
          case MLAST: {
            if (last_ != rec) {
              if (first_ == rec) first_ = rec->next;
              if (rec->prev) rec->prev->next = rec->next;
              if (rec->next) rec->next->prev = rec->prev;
              rec->prev = last_;
              rec->next = NULL;
              last_->next = rec;
              last_ = rec;
            }
            break;
          }
        }
        return &rec->value;
      } else {
        rec = rec->child;
      }
    }
    return NULL;
  }
  /**
   * Remove all records.
   */
  void clear() {
    if (count_ < 1) return;
    Record* rec = last_;
    while (rec) {
      Record* prev = rec->prev;
      delete rec;
      rec = prev;
    }
    for (size_t i = 0; i < bnum_; i++) {
      buckets_[i] = NULL;
    }
    first_ = NULL;
    last_ = NULL;
    count_ = 0;
  }
  /**
   * Get the number of records.
   */
  size_t count() {
    return count_;
  }
  /**
   * Get an iterator at the first record.
   */
  Iterator begin() {
    return Iterator(this, first_);
  }
  /**
   * Get an iterator of the end sentry.
   */
  Iterator end() {
    return Iterator(this, NULL);
  }
  /**
   * Get an iterator at a record.
   * @param key the key.
   * @return the pointer to the value of the corresponding record, or NULL on failure.
   */
  Iterator find(const KEY& key) {
    size_t bidx = hash_(key) % bnum_;
    Record* rec = buckets_[bidx];
    while (rec) {
      if (equalto_(rec->key, key)) {
        return Iterator(this, rec);
      } else {
        rec = rec->child;
      }
    }
    return Iterator(this, NULL);
  }
  /**
   * Get the reference of the key of the first record.
   * @return the reference of the key of the first record.
   */
  const KEY& first_key() {
    return first_->key;
  }
  /**
   * Get the reference of the value of the first record.
   * @return the reference of the value of the first record.
   */
  VALUE& first_value() {
    return first_->value;
  }
  /**
   * Get the reference of the key of the last record.
   * @return the reference of the key of the last record.
   */
  const KEY& last_key() {
    return last_->key;
  }
  /**
   * Get the reference of the value of the last record.
   * @return the reference of the value of the last record.
   */
  VALUE& last_value() {
    return last_->value;
  }
private:
  /**
   * Record data.
   */
  struct Record {
    KEY key;                             ///< key
    VALUE value;                         ///< value
    Record* child;                       ///< child record
    Record* prev;                        ///< previous record
    Record* next;                        ///< next record
    /** constructor */
    Record(const KEY& k, const VALUE& v) :
      key(k), value(v), child(NULL), prev(NULL), next(NULL) {}
  };
  /**
   * Initialize fields.
   */
  void initialize() {
    if (bnum_ >= LHMZMAPBNUM) {
      buckets_ = (Record**)mapalloc(sizeof(*buckets_) * bnum_);
    } else {
      buckets_ = new Record*[bnum_];
      for (size_t i = 0; i < bnum_; i++) {
        buckets_[i] = NULL;
      }
    }
  }
  /**
   * Clean up fields.
   */
  void destroy() {
    Record* rec = last_;
    while (rec) {
      Record* prev = rec->prev;
      delete rec;
      rec = prev;
    }
    if (bnum_ >= LHMZMAPBNUM) {
      mapfree(buckets_);
    } else {
      delete[] buckets_;
    }
  }
  /** The functor of the hash function. */
  HASH hash_;
  /** The functor of the hash function. */
  EQUALTO equalto_;
  /** The bucket array. */
  Record** buckets_;
  /** The number of buckets. */
  size_t bnum_;
  /** The first record. */
  Record* first_;
  /** The last record. */
  Record* last_;
  /** The number of records. */
  size_t count_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
