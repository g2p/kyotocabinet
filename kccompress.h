/*************************************************************************************************
 * Data compressor and decompressor
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


#ifndef _KCCOMPRESS_H                    // duplication check
#define _KCCOMPRESS_H

#include <kccommon.h>
#include <kcutil.h>
#include <kcthread.h>

namespace kyotocabinet {                 // common namespace


/**
 * Interfrace of data compression and decompression.
 */
class Compressor {
public:
  /**
   * Destructor.
   */
  virtual ~Compressor() {}
  /**
   * Compress a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return the pointer to the result data, or NULL on failure.
   * @note Because the region of the return value is allocated with the the new[] operator, it
   * should be released with the delete[] operator when it is no longer in use.
   */
  virtual char* compress(const void* buf, size_t size, size_t* sp) = 0;
  /**
   * Decompress a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return the pointer to the result data, or NULL on failure.
   * @note Because an additional zero code is appended at the end of the region of the return
   * value, the return value can be treated as a C-style string.  Because the region of the
   * return value is allocated with the the new[] operator, it should be released with the
   * delete[] operator when it is no longer in use.
   */
  virtual char* decompress(const void* buf, size_t size, size_t* sp) = 0;
};


/**
 * Zlib compressor.
 */
class Zlib {
public:
  /**
   * Compression modes.
   */
  enum Mode {
    RAW,                                 ///< without any checksum
    DEFLATE,                             ///< with Adler32 checksum
    GZIP                                 ///< with CRC32 checksum and various meta data
  };
  /**
   * Compress a serial data.
   * @param mode the compression mode.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return the pointer to the result data, or NULL on failure.
   * @note Because the region of the return value is allocated with the the new[] operator, it
   * should be released with the delete[] operator when it is no longer in use.
   */
  static char* compress(Mode mode, const void* buf, size_t size, size_t* sp);
  /**
   * Decompress a serial data.
   * @param mode the compression mode.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return the pointer to the result data, or NULL on failure.
   * @note Because an additional zero code is appended at the end of the region of the return
   * value, the return value can be treated as a C-style string.  Because the region of the
   * return value is allocated with the the new[] operator, it should be released with the
   * delete[] operator when it is no longer in use.
   */
  static char* decompress(Mode mode, const void* buf, size_t size, size_t* sp);
};


/**
 * Compressor with the Zlib raw mode.
 */
class ZlibRawCompressor : public Compressor {
  char* compress(const void* buf, size_t size, size_t* sp) {
    _assert_(buf && sp);
    return Zlib::compress(Zlib::RAW, buf, size, sp);
  }
  char* decompress(const void* buf, size_t size, size_t* sp) {
    _assert_(buf && sp);
    return Zlib::decompress(Zlib::RAW, buf, size, sp);
  }
};


/**
 * Compressor with the Zlib deflate mode.
 */
class ZlibDeflateCompressor : public Compressor {
  char* compress(const void* buf, size_t size, size_t* sp) {
    _assert_(buf && sp);
    return Zlib::compress(Zlib::DEFLATE, buf, size, sp);
  }
  char* decompress(const void* buf, size_t size, size_t* sp) {
    _assert_(buf && sp);
    return Zlib::decompress(Zlib::DEFLATE, buf, size, sp);
  }
};


/**
 * Compressor with the Zlib gzip mode.
 */
class ZlibGzipCompressor : public Compressor {
  char* compress(const void* buf, size_t size, size_t* sp) {
    _assert_(buf && sp);
    return Zlib::compress(Zlib::GZIP, buf, size, sp);
  }
  char* decompress(const void* buf, size_t size, size_t* sp) {
    _assert_(buf && sp);
    return Zlib::decompress(Zlib::GZIP, buf, size, sp);
  }
};


/**
 * Prepared variable of the compressor with the Zlib raw mode.
 */
extern ZlibRawCompressor ZLIBRAWCOMP;


/**
 * Prepared variable of the compressor with the Zlib deflate mode.
 */
extern ZlibDeflateCompressor ZLIBDEFLCOMP;


/**
 * Prepared variable of the compressor with the Zlib gzip mode.
 */
extern ZlibGzipCompressor ZLIBGZIPCOMP;


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
