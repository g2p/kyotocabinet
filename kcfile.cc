/*************************************************************************************************
 * Filesystem abstraction
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


#include "kcfile.h"
#include "myconf.h"

namespace kyotocabinet {                 // common namespace


/**
 * Constants for implementation.
 */
namespace {
const int32_t FILEPERM = 00644;          ///< default permission of a new file
const int32_t IOBUFSIZ = 1024;           ///< size of the IO buffer
const char* WALPATHEXT = "wal";          ///< extension of the WAL file
const char WALMAGICDATA[] = "KW\n";      ///< magic data of the WAL file
const int32_t WALMAPSIZ = 256 << 10;     ///< size of the IO buffer
const uint8_t WALMSGMAGIC = 0xee;        ///< magic data for WAL record
}


/**
 * File internal.
 */
struct FileCore {
  Mutex alock;                           ///< attribute lock
  TSDKey errmsg;                         ///< error message
  int fd;                                ///< file descriptor
  char* map;                             ///< mapped memory
  int64_t msiz;                          ///< map size
  int64_t lsiz;                          ///< logical size
  int64_t psiz;                          ///< physical size
  std::string path;                      ///< file path
  int walfd;                             ///< file descriptor for WAL
  char* walmap;                          ///< mapped memory for the WAL
  int64_t walsiz;                        ///< size of WAL
  bool tran;                             ///< whether in transaction
  bool trhard;                           ///< whether hard transaction
  int64_t trbase;                        ///< base offset of guarded region
  int64_t trmsiz;                        ///< minimum size during transaction
};


/**
 * WAL message.
 */
struct WALMessage {
  int64_t off;                           ///< offset of the region
  std::string body;                      ///< body data
};


/**
 * Set the error message.
 * @param core the inner condition.
 * @param msg the error message.
 */
static void seterrmsg(FileCore* core, const char* msg);


/**
 * Get the path of the WAL file.
 * @param path the path of the destination file.
 * @return the path of the WAL file.
 */
static std::string walpath(const std::string& path);


/**
 * Write a log message into the WAL file.
 * @param core the inner condition.
 * @param off the offset of the destination.
 * @param size the size of the data region.
 * @param base the base offset.
 * @return true on success, or false on failure.
 */
static bool walwrite(FileCore *core, int64_t off, size_t size, int64_t base);


/**
 * Apply log messages in the WAL file.
 * @param core the inner condition.
 * @return true on success, or false on failure.
 */
static bool walapply(FileCore* core);


/**
 * Write data into a file.
 * @param fd the file descriptor.
 * @param off the offset of the destination.
 * @param buf the pointer to the data region.
 * @param size the size of the data region.
 * @return true on success, or false on failure.
 */
static bool mywrite(int fd, int64_t off, const void* buf, size_t size);


/**
 * Read data from a file.
 * @param fd the file descriptor.
 * @param buf the pointer to the destination region.
 * @param size the size of the data to be read.
 * @return true on success, or false on failure.
 */
static size_t myread(int fd, void* buf, size_t count);


/** Path delimiter character. */
const char File::PATHCHR = MYPATHCHR;


/** Path delimiter string. */
const char* const File::PATHSTR = MYPATHSTR;


/** Extension delimiter character. */
const char File::EXTCHR = MYEXTCHR;


/** Extension delimiter string. */
const char* const File::EXTSTR = MYEXTSTR;


/** Current directory string. */
const char* const File::CDIRSTR = MYCDIRSTR;


/** Parent directory string. */
const char* const File::PDIRSTR = MYPDIRSTR;


/**
 * Default constructor.
 */
File::File() : opq_(NULL) {
  FileCore* core = new FileCore;
  core->fd = -1;
  core->map = NULL;
  core->msiz = 0;
  core->lsiz = 0;
  core->psiz = 0;
  core->walfd = -1;
  core->walmap = NULL;
  core->walsiz = 0;
  core->tran = false;
  core->trhard = false;
  core->trmsiz = 0;
  opq_ = core;
}


/**
 * Destructor.
 */
File::~File() {
  FileCore* core = (FileCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
}


/**
 * Get the last happened error information.
 */
const char* File::error() const {
  FileCore* core = (FileCore*)opq_;
  const char* msg = (const char*)core->errmsg.get();
  if (!msg) msg = "no error";
  return msg;
}


/**
 * Open a file.
 */
bool File::open(const std::string& path, uint32_t mode, int64_t msiz) {
  FileCore* core = (FileCore*)opq_;
  int oflags = O_RDONLY;
  if (mode & OWRITER) {
    oflags = O_RDWR;
    if (mode & OCREATE) oflags |= O_CREAT;
    if (mode & OTRUNCATE) oflags |= O_TRUNC;
  }
  int fd = ::open(path.c_str(), oflags, FILEPERM);
  if (fd < 0) {
    switch (errno) {
      case EACCES: seterrmsg(core, "open failed (permission denied)"); break;
      case ENOENT: seterrmsg(core, "open failed (file not found)"); break;
      case ENOTDIR: seterrmsg(core, "open failed (invalid path)"); break;
      default: seterrmsg(core, "open failed"); break;
    }
    return false;
  }
  if (!(mode & ONOLOCK)) {
    struct flock flbuf;
    std::memset(&flbuf, 0, sizeof(flbuf));
    flbuf.l_type = mode & OWRITER ? F_WRLCK : F_RDLCK;
    flbuf.l_whence = SEEK_SET;
    flbuf.l_start = 0;
    flbuf.l_len = 0;
    flbuf.l_pid = 0;
    int cmd = mode & OTRYLOCK ? F_SETLK : F_SETLKW;
    while (fcntl(fd, cmd, &flbuf) != 0) {
      if (errno != EINTR) {
        seterrmsg(core, "fcntl failed");
        ::close(fd);
        return false;
      }
    }
  }
  struct ::stat sbuf;
  if (::fstat(fd, &sbuf) != 0) {
    seterrmsg(core, "fstat failed");
    ::close(fd);
    return false;
  }
  if (!(mode & OWRITER) || !(mode & OTRUNCATE)) {
    std::string wpath = walpath(path);
    struct ::stat wsbuf;
    if (::stat(wpath.c_str(), &wsbuf) == 0 && wsbuf.st_size >= (int64_t)sizeof(WALMAGICDATA) &&
        wsbuf.st_uid == sbuf.st_uid && !(mode & ONOLOCK)) {
      int walfd = ::open(wpath.c_str(), O_RDWR, FILEPERM);
      if (walfd >= 0) {
        char mbuf[sizeof(WALMAGICDATA)];
        if (myread(walfd, mbuf, sizeof(mbuf)) &&
            !std::memcmp(mbuf, WALMAGICDATA, sizeof(WALMAGICDATA))) {
          int ofd = mode & OWRITER ? fd : ::open(path.c_str(), O_WRONLY, FILEPERM);
          if (ofd >= 0) {
            core->fd = ofd;
            core->walfd = walfd;
            walapply(core);
            if (::ftruncate(walfd, 0) != 0) seterrmsg(core, "ftruncate failed");
            core->fd = -1;
            core->walfd = -1;
            if (::fstat(fd, &sbuf) != 0) {
              seterrmsg(core, "fstat failed");
              ::close(fd);
              return false;
            }
            if (ofd != fd && ::close(ofd) != 0) seterrmsg(core, "close failed");

            // hoge
            std::cerr << "[recovered]" << std::endl;
            Thread::sleep(0.5);


          } else {
            seterrmsg(core, "open failed");
          }
        }
        if (::close(walfd) != 0) seterrmsg(core, "close failed");
        if (::lstat(wpath.c_str(), &wsbuf) == 0 && S_ISREG(wsbuf.st_mode) &&
            ::unlink(wpath.c_str()) != 0) seterrmsg(core, "unlink failed");
      }
    }
  }
  int64_t lsiz = sbuf.st_size;
  int64_t psiz = lsiz;
  int mprot = PROT_READ;
  if (mode & OWRITER) {
    mprot |= PROT_WRITE;
  } else if (msiz > lsiz) {
    msiz = lsiz;
  }
  int64_t diff = msiz % PAGESIZE;
  if (diff > 0) msiz += PAGESIZE - diff;
  void* map = NULL;
  if (msiz > 0) {
    map = ::mmap(0, msiz, mprot, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
      seterrmsg(core, "mmap failed");
      ::close(fd);
      return false;
    }
  }
  core->fd = fd;
  core->map = (char*)map;
  core->lsiz = lsiz;
  core->msiz = msiz;
  core->psiz = psiz;
  core->path.append(path);
  return true;
}


/**
 * Close the file.
 */
bool File::close() {
  FileCore* core = (FileCore*)opq_;
  bool err = false;
  if (core->tran && !end_transaction(false)) err = true;
  if (core->walfd >= 0) {
    if (::munmap(core->walmap, WALMAPSIZ) != 0) {
      seterrmsg(core, "munmap failed");
      err = true;
    }
    if (::close(core->walfd) != 0) {
      seterrmsg(core, "close failed");
      err = true;
    }
    std::string wpath = walpath(core->path);
    struct ::stat sbuf;
    if (::lstat(wpath.c_str(), &sbuf) == 0 && S_ISREG(sbuf.st_mode) &&
        ::unlink(wpath.c_str()) != 0) {
      seterrmsg(core, "unlink failed");
      err = true;
    }
  }
  if (core->msiz > 0 && ::munmap(core->map, core->msiz) != 0) {
    seterrmsg(core, "munmap failed");
    err = true;
  }
  if (core->psiz != core->lsiz && ::ftruncate(core->fd, core->lsiz) != 0) {
    seterrmsg(core, "ftruncate failed");
    err = true;
  }
  if (::close(core->fd) != 0) {
    seterrmsg(core, "close failed");
    err = true;
  }
  core->fd = -1;
  core->map = NULL;
  core->lsiz = 0;
  core->msiz = 0;
  core->psiz = 0;
  core->path.clear();
  core->walfd = -1;
  core->walmap = NULL;
  core->walsiz = 0;
  core->tran = false;
  core->trhard = false;
  core->trmsiz = 0;
  return !err;
}


/**
 * Write data.
 */
bool File::write(int64_t off, const void* buf, size_t size) {
  if (size < 1) return true;
  FileCore* core = (FileCore*)opq_;
  if (core->tran && !walwrite(core, off, size, core->trbase)) return false;
  int64_t end = off + size;
  core->alock.lock();
  if (end <= core->msiz) {
    if (end > core->psiz) {
      int64_t psiz = end + core->psiz / 2;
      int64_t diff = psiz % PAGESIZE;
      if (diff > 0) psiz += PAGESIZE - diff;
      if (psiz > core->msiz) psiz = core->msiz;
      if (::ftruncate(core->fd, psiz) != 0) {
        seterrmsg(core, "ftruncate failed");
        core->alock.unlock();
        return false;
      }
      core->psiz = psiz;
    }
    if (end > core->lsiz) core->lsiz = end;
    core->alock.unlock();
    std::memcpy(core->map + off, buf, size);
    return true;
  }
  if (off < core->msiz) {
    if (end > core->psiz) {
      if (::ftruncate(core->fd, end) != 0) {
        seterrmsg(core, "ftruncate failed");
        core->alock.unlock();
        return false;
      }
      core->psiz = end;
    }
    size_t hsiz = core->msiz - off;
    std::memcpy(core->map + off, buf, hsiz);
    off += hsiz;
    buf = (char*)buf + hsiz;
    size -= hsiz;
  }
  if (end > core->lsiz) core->lsiz = end;
  if (end > core->psiz) {
    if (core->psiz < core->msiz && ::ftruncate(core->fd, core->msiz) != 0) {
      seterrmsg(core, "ftruncate failed");
      core->alock.unlock();
      return false;
    }
    core->psiz = end;
  }
  core->alock.unlock();
  if (!mywrite(core->fd, off, buf, size)) {
    seterrmsg(core, "mywrite failed");
    return false;
  }
  return true;
}


/**
 * Write data with assuring the region does not spill from the file size.
 */
bool File::write_fast(int64_t off, const void* buf, size_t size) {
  FileCore* core = (FileCore*)opq_;
  if (core->tran && !walwrite(core, off, size, core->trbase)) return false;
  int64_t end = off + size;
  if (end <= core->msiz) {
    std::memcpy(core->map + off, buf, size);
    return true;
  }
  if (off < core->msiz) {
    size_t hsiz = core->msiz - off;
    std::memcpy(core->map + off, buf, hsiz);
    off += hsiz;
    buf = (char*)buf + hsiz;
    size -= hsiz;
  }
  if (!mywrite(core->fd, off, buf, size)) {
    seterrmsg(core, "mywrite failed");
    return false;
  }
  return true;
}


/**
 * Write data at the end of the file.
 */
bool File::append(const void* buf, size_t size) {
  if (size < 1) return true;
  FileCore* core = (FileCore*)opq_;
  core->alock.lock();
  int64_t off = core->lsiz;
  int64_t end = off + size;
  if (end <= core->msiz) {
    if (end > core->psiz) {
      int64_t psiz = end + core->psiz / 2;
      int64_t diff = psiz % PAGESIZE;
      if (diff > 0) psiz += PAGESIZE - diff;
      if (psiz > core->msiz) psiz = core->msiz;
      if (::ftruncate(core->fd, psiz) != 0) {
        seterrmsg(core, "ftruncate failed");
        core->alock.unlock();
        return false;
      }
      core->psiz = psiz;
    }
    core->lsiz = end;
    core->alock.unlock();
    std::memcpy(core->map + off, buf, size);
    return true;
  }
  if (off < core->msiz) {
    if (end > core->psiz) {
      if (::ftruncate(core->fd, end) != 0) {
        seterrmsg(core, "ftruncate failed");
        core->alock.unlock();
        return false;
      }
      core->psiz = end;
    }
    size_t hsiz = core->msiz - off;
    std::memcpy(core->map + off, buf, hsiz);
    off += hsiz;
    buf = (char*)buf + hsiz;
    size -= hsiz;
  }
  core->lsiz = end;
  core->psiz = end;
  core->alock.unlock();
  while (true) {
    ssize_t wb = ::pwrite(core->fd, buf, size, off);
    if (wb >= (ssize_t)size) {
      return true;
    } else if (wb > 0) {
      buf = (char*)buf + wb;
      size -= wb;
      off += wb;
    } else if (wb == -1) {
      if (errno != EINTR) {
        seterrmsg(core, "pwrite failed");
        return false;
      }
    } else if (size > 0) {
      seterrmsg(core, "pwrite failed");
      return false;
    }
  }
  return true;
}


/**
 * Read data.
 */
bool File::read(int64_t off, void* buf, size_t size) {
  if (size < 1) return true;
  FileCore* core = (FileCore*)opq_;
  int64_t end = off + size;
  core->alock.lock();
  if (end > core->lsiz) {
    seterrmsg(core, "out of bounds");
    core->alock.unlock();
    return false;
  }
  core->alock.unlock();
  if (end <= core->msiz) {
    std::memcpy(buf, core->map + off, size);
    return true;
  }
  if (off < core->msiz) {
    int hsiz = core->msiz - off;
    std::memcpy(buf, core->map + off, hsiz);
    off += hsiz;
    buf = (char*)buf + hsiz;
    size -= hsiz;
  }
  while (true) {
    ssize_t rb = ::pread(core->fd, buf, size, off);
    if (rb >= (ssize_t)size) {
      break;
    } else if (rb > 0) {
      buf = (char*)buf + rb;
      size -= rb;
      off += rb;
    } else if (rb == -1) {
      if (errno != EINTR) {
        seterrmsg(core, "pread failed");
        return false;
      }
    } else if (size > 0) {
      Thread::yield();
    }
  }
  return true;
}


/**
 * Read data with assuring the region does not spill from the file size.
 */
bool File::read_fast(int64_t off, void* buf, size_t size) {
  FileCore* core = (FileCore*)opq_;
  int64_t end = off + size;
  if (end <= core->msiz) {
    std::memcpy(buf, core->map + off, size);
    return true;
  }
  if (off < core->msiz) {
    int hsiz = core->msiz - off;
    std::memcpy(buf, core->map + off, hsiz);
    off += hsiz;
    buf = (char*)buf + hsiz;
    size -= hsiz;
  }
  while (true) {
    ssize_t rb = ::pread(core->fd, buf, size, off);
    if (rb >= (ssize_t)size) {
      break;
    } else if (rb > 0) {
      buf = (char*)buf + rb;
      size -= rb;
      off += rb;
      Thread::yield();
    } else if (rb == -1) {
      if (errno != EINTR) {
        seterrmsg(core, "pread failed");
        return false;
      }
    } else if (size > 0) {
      Thread::yield();
    }
  }
  return true;
}


/**
 * Truncate the file.
 */
bool File::truncate(int64_t size) {
  FileCore* core = (FileCore*)opq_;
  if (core->tran && size < core->trmsiz) {
    if (!walwrite(core, size, core->trmsiz - size, core->trbase)) return false;
    core->trmsiz = size;
  }
  core->alock.lock();
  if (::ftruncate(core->fd, size) != 0) {
    seterrmsg(core, "ftruncate failed");
    core->alock.unlock();
    return false;
  }
  core->lsiz = size;
  core->psiz = size;
  core->alock.unlock();
  return true;
}


/**
 * Synchronize updated contents with the file and the device.
 */
bool File::synchronize(bool hard) {
  FileCore* core = (FileCore*)opq_;
  bool err = false;
  core->alock.lock();
  if (hard && core->msiz > 0) {
    int64_t msiz = core->msiz;
    if (msiz > core->psiz) msiz = core->psiz;
    if (msiz > 0 && ::msync(core->map, msiz, MS_SYNC) != 0) {
      seterrmsg(core, "msync failed");
      err = true;
    }
  }
  if (::ftruncate(core->fd, core->lsiz) != 0) {
    seterrmsg(core, "ftruncate failed");
    err = true;
  }
  if (core->psiz > core->lsiz) core->psiz = core->lsiz;
  if (hard && ::fsync(core->fd) != 0) {
    seterrmsg(core, "fsync failed");
    err = true;
  }
  core->alock.unlock();
  return !err;
}


/**
 * Refresh the internal state for update by others.
  */
bool File::refresh() {
  FileCore* core = (FileCore*)opq_;
  struct ::stat sbuf;
  if (::fstat(core->fd, &sbuf) != 0) {
    seterrmsg(core, "fstat failed");
    return false;
  }
  core->lsiz = sbuf.st_size;
  core->psiz = sbuf.st_size;
  bool err = false;
  int64_t msiz = core->msiz;
  if (msiz > core->psiz) msiz = core->psiz;
  if (::msync(core->map, msiz, MS_INVALIDATE) != 0) {
    seterrmsg(core, "msync failed");
    err = true;
  }
  return !err;
}


/**
 * Begin transaction.
 */
bool File::begin_transaction(bool hard, int64_t off) {
  FileCore* core = (FileCore*)opq_;
  bool err = false;
  core->alock.lock();
  if (core->walfd < 0) {
    std::string wpath = walpath(core->path);
    int fd = ::open(wpath.c_str(), O_RDWR | O_CREAT | O_TRUNC, FILEPERM);
    if (fd < 0) {
      switch (errno) {
        case EACCES: seterrmsg(core, "open failed (permission denied)"); break;
        case ENOENT: seterrmsg(core, "open failed (file not found)"); break;
        case ENOTDIR: seterrmsg(core, "open failed (invalid path)"); break;
        default: seterrmsg(core, "open failed"); break;
      }
      core->alock.unlock();
      return false;
    }
    if (::ftruncate(fd, WALMAPSIZ) != 0) {
      seterrmsg(core, "ftrunate failed");
      ::close(fd);
      return false;
    }
    if (hard && ::fsync(fd) != 0) {
      seterrmsg(core, "fsync failed");
      ::close(fd);
      return false;
    }
    void* map = ::mmap(0, WALMAPSIZ, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
      seterrmsg(core, "mmap failed");
      ::close(fd);
      return false;
    }
    core->walfd = fd;
    core->walmap = (char*)map;
  }
  char rbuf[IOBUFSIZ];
  char* wp = rbuf;
  std::memcpy(wp, WALMAGICDATA, sizeof(WALMAGICDATA));
  wp += sizeof(WALMAGICDATA);
  int64_t num = hton64(core->lsiz);
  std::memcpy(wp, &num, sizeof(num));
  wp += sizeof(num);
  if (!mywrite(core->walfd, 0, rbuf, wp - rbuf)) {
    seterrmsg(core, "mywrite failed");
    err = true;
  }
  core->walsiz = wp - rbuf;
  if (err) {
    core->alock.unlock();
    return false;
  }
  core->tran = true;
  core->trhard = hard;
  core->trbase = off;
  core->trmsiz = core->lsiz;
  core->alock.unlock();
  return true;
}


/**
 * Commit transaction.
 */
bool File::end_transaction(bool commit) {
  FileCore* core = (FileCore*)opq_;
  bool err = false;
  core->alock.lock();
  if (!commit && !walapply(core)) err = true;
  if (!err) {
    std::memset(core->walmap, 0, core->walsiz < WALMAPSIZ ? core->walsiz : WALMAPSIZ);
    if (core->walsiz > WALMAPSIZ && ::ftruncate(core->walfd, WALMAPSIZ) != 0) {
      seterrmsg(core, "ftruncate failed");
      err = true;
    }
  }
  if (core->trhard) {
    int64_t msiz = core->msiz;
    if (msiz > core->psiz) msiz = core->psiz;
    if (msiz > 0 && ::msync(core->map, msiz, MS_SYNC) != 0) {
      seterrmsg(core, "msync failed");
      err = true;
    }
    if (::fsync(core->fd) != 0) {
      seterrmsg(core, "fsync failed");
      err = true;
    }
    if (::msync(core->walmap, 1, MS_SYNC) != 0) {
      seterrmsg(core, "msync failed");
      err = true;
    }
    if (core->walsiz > WALMAPSIZ && ::fsync(core->walfd) != 0) {
      seterrmsg(core, "fsync failed");
      err = true;
    }
  }
  core->tran = false;
  core->alock.unlock();
  return !err;
}


/**
 * Write a WAL message of transaction explicitly.
 */
bool File::write_transaction(int64_t off, size_t size) {
  FileCore* core = (FileCore*)opq_;
  return walwrite(core, off, size, 0);
}


/**
 * Get the size of the file.
 */
int64_t File::size() const {
  FileCore* core = (FileCore*)opq_;
  return core->lsiz;
}


/**
 * Get the path of the file.
 */
std::string File::path() const {
  FileCore* core = (FileCore*)opq_;
  return core->path;
}


/**
 * Get the status information of a file.
 */
bool File::status(const std::string& path, Status* buf) {
  struct ::stat sbuf;
  if (::stat(path.c_str(), &sbuf) != 0) return false;
  buf->isdir = S_ISDIR(sbuf.st_mode);
  buf->size = sbuf.st_size;
  buf->mtime = sbuf.st_mtime;
  return true;
}


/**
 * Remove a file.
 */
bool File::remove(const std::string& path) {
  return ::unlink(path.c_str()) == 0;
}


/**
 * Set the error message.
 */
static void seterrmsg(FileCore* core, const char* msg) {
  core->errmsg.set((void*)msg);
}


/**
 * Get the path of the WAL file.
 */
static std::string walpath(const std::string& path) {
  return path + File::EXTCHR + WALPATHEXT;
}


/**
 * Write a log message into the WAL file.
 */
static bool walwrite(FileCore *core, int64_t off, size_t size, int64_t base) {
  bool err = false;
  if (off < base) {
    int64_t diff = base - off;
    if (diff >= (int64_t)size) return true;
    off = base;
    size -= diff;
  }
  int64_t rem = core->trmsiz - off;
  if (rem < 1) return true;
  if (rem < (int64_t)size) size = rem;
  char stack[IOBUFSIZ];
  size_t rsiz = sizeof(int8_t) + sizeof(int64_t) * 2 + size;
  char* rbuf = rsiz > sizeof(stack) ? new char[rsiz] : stack;
  char* wp = rbuf;
  *(wp++) = WALMSGMAGIC;
  int64_t num = hton64(off);
  std::memcpy(wp, &num, sizeof(num));
  wp += sizeof(num);
  num = hton64(size);
  std::memcpy(wp, &num, sizeof(num));
  wp += sizeof(num);
  core->alock.lock();
  int64_t end = off + size;
  if (end <= core->msiz) {
    std::memcpy(wp, core->map + off, size);
  } else {
    if (off < core->msiz) {
      int hsiz = core->msiz - off;
      std::memcpy(wp, core->map + off, hsiz);
      off += hsiz;
      wp += hsiz;
      size -= hsiz;
    }
    while (true) {
      ssize_t rb = ::pread(core->fd, wp, size, off);
      if (rb >= (ssize_t)size) {
        break;
      } else if (rb > 0) {
        wp += rb;
        size -= rb;
        off += rb;
      } else if (rb == -1) {
        if (errno != EINTR) {
          err = true;
          break;
        }
      } else {
        err = true;
        break;
      }
    }
    if (err) {
      seterrmsg(core, "pread failed");
      std::memset(wp, 0, size);
    }
  }
  end = core->walsiz + rsiz;
  if (end <= WALMAPSIZ) {
    std::memcpy(core->walmap + core->walsiz, rbuf, rsiz);
    if (core->trhard && ::msync(core->walmap, end, MS_SYNC) != 0) {
      seterrmsg(core, "msync failed");
      err = true;
    }
  } else {
    const char* rp = rbuf;
    if (core->walsiz < WALMAPSIZ) {
      size_t hsiz = WALMAPSIZ - core->walsiz;
      std::memcpy(core->walmap + core->walsiz, rp, hsiz);
      if (core->trhard && ::msync(core->walmap, WALMAPSIZ, MS_SYNC) != 0) {
        seterrmsg(core, "msync failed");
        err = true;
      }
      core->walsiz += hsiz;
      rp += hsiz;
      rsiz -= hsiz;
    }
    if (!mywrite(core->walfd, core->walsiz, rp, rsiz)) {
      seterrmsg(core, "mywrite failed");
      err = true;
    }
    if (core->trhard && ::fsync(core->walfd) != 0) {
      seterrmsg(core, "fsync failed");
      err = true;
    }
  }
  core->walsiz = end;
  if (rbuf != stack) delete[] rbuf;
  core->alock.unlock();
  return !err;
}


/**
 * Apply log messages in the WAL file.
 */
static bool walapply(FileCore* core) {
  bool err = false;
  char buf[IOBUFSIZ];
  int64_t hsiz = sizeof(WALMAGICDATA) + sizeof(int64_t);
  int64_t rem = ::lseek(core->walfd, 0, SEEK_END);
  if (rem < hsiz) {
    seterrmsg(core, "lseek failed");
    return false;
  }
  if (::lseek(core->walfd, 0, SEEK_SET) != 0) {
    seterrmsg(core, "lseek failed");
    return false;
  }
  if (!myread(core->walfd, buf, hsiz)) {
    seterrmsg(core, "myread failed");
    return false;
  }
  if (*buf == 0) return true;
  if (std::memcmp(buf, WALMAGICDATA, sizeof(WALMAGICDATA))) {
    seterrmsg(core, "invalid magic data of WAL");
    return false;
  }
  int64_t osiz;
  std::memcpy(&osiz, buf + sizeof(WALMAGICDATA), sizeof(osiz));
  osiz = ntoh64(osiz);
  rem -= hsiz;
  hsiz = sizeof(uint8_t) + sizeof(int64_t) * 2;
  std::vector<WALMessage> msgs;
  int end = 0;
  while (rem >= hsiz) {
    if (!myread(core->walfd, buf, hsiz)) {
      seterrmsg(core, "myread failed");
      err = true;
      break;
    }
    if (*buf == 0) {
      rem = 0;
      break;
    }
    rem -= hsiz;
    char* rp = buf;
    if (*(uint8_t*)(rp++) != WALMSGMAGIC) {
      seterrmsg(core, "invalid magic data of WAL message");
      err = true;
      break;
    }
    if (rem > 0) {
      int64_t off;
      std::memcpy(&off, rp, sizeof(off));
      off = ntoh64(off);
      rp += sizeof(off);
      int64_t size;
      std::memcpy(&size, rp, sizeof(size));
      size = ntoh64(size);
      rp += sizeof(size);
      if (off < 0 || size < 0) {
        seterrmsg(core, "invalid meta data of WAL message");
        err = true;
        break;
      }
      if (rem < size) {
        seterrmsg(core, "too short WAL message");
        err = true;
        break;
      }
      char* rbuf = size > (int64_t)sizeof(buf) ? new char[size] : buf;
      if (!myread(core->walfd, rbuf, size)) {
        seterrmsg(core, "myread failed");
        if (rbuf != buf) delete[] rbuf;
        err = true;
        break;
      }
      rem -= size;
      WALMessage msg = { off, std::string(rbuf, size) };
      msgs.push_back(msg);
      if (off + size > end) end = off + size;
      if (rbuf != buf) delete[] rbuf;
    }
  }
  if (rem != 0) {
    if (!myread(core->walfd, buf, 1)) {
      seterrmsg(core, "myread failed");
      err = true;
    } else if (*buf != 0) {
      seterrmsg(core, "too few messages of WAL");
      err = true;
    }
  }
  if (end > core->msiz) end = core->msiz;
  if (core->psiz < end && ::ftruncate(core->fd, end) != 0) {
    seterrmsg(core, "ftruncate failed");
    err = true;
  }
  for (int64_t i = (int64_t)msgs.size() - 1; i >= 0; i--) {
    const WALMessage& msg = msgs[i];
    int64_t off = msg.off;
    const char* rbuf = msg.body.c_str();
    size_t size = msg.body.size();
    int64_t end = off + size;
    if (end <= core->msiz) {
      std::memcpy(core->map + off, rbuf, size);
    } else {
      if (off < core->msiz) {
        size_t hsiz = core->msiz - off;
        std::memcpy(core->map + off, rbuf, hsiz);
        off += hsiz;
        rbuf += hsiz;
        size -= hsiz;
      }
      while (true) {
        ssize_t wb = ::pwrite(core->fd, rbuf, size, off);
        if (wb >= (ssize_t)size) {
          break;
        } else if (wb > 0) {
          rbuf += wb;
          size -= wb;
          off += wb;
        } else if (wb == -1) {
          if (errno != EINTR) {
            seterrmsg(core, "pwrite failed");
            err = true;
            break;
          }
        } else if (size > 0) {
          seterrmsg(core, "pwrite failed");
          err = true;
          break;
        }
      }
    }
  }
  if (::ftruncate(core->fd, osiz) == 0) {
    core->lsiz = osiz;
    core->psiz = osiz;
  } else {
    seterrmsg(core, "ftruncate failed");
    err = true;
  }
  return !err;
}


/**
 * Write data into a file.
 */
static bool mywrite(int fd, int64_t off, const void* buf, size_t size) {
  while (true) {
    ssize_t wb = ::pwrite(fd, buf, size, off);
    if (wb >= (ssize_t)size) {
      return true;
    } else if (wb > 0) {
      buf = (char*)buf + wb;
      size -= wb;
      off += wb;
    } else if (wb == -1) {
      if (errno != EINTR) return false;
    } else if (size > 0) {
      return false;
    }
  }
  return true;
}


/**
 * Read data from a file.
 */
static size_t myread(int fd, void* buf, size_t size) {
  while (true) {
    ssize_t rb = ::read(fd, buf, size);
    if (rb >= (ssize_t)size) {
      break;
    } else if (rb > 0) {
      buf = (char*)buf + rb;
      size -= rb;
    } else if (rb == -1) {
      if (errno != EINTR) return false;
    } else if (size > 0) {
      return false;
    }
  }
  return true;
}


}                                        // common namespace

// END OF FILE
