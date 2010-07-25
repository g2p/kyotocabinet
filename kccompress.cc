/*************************************************************************************************
 * Data compressor and decompressor
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


#include "kccompress.h"
#include "myconf.h"

#if defined(_KC_ZLIB)
extern "C" {
#include <zlib.h>
}
#endif

namespace kyotocabinet {                 // common namespace


/**
 * Compress a serial data.
 */
char* Zlib::compress(const void* buf, size_t size, size_t* sp, Mode mode) {
#if defined(_KC_ZLIB)
  _assert_(buf && size <= MEMMAXSIZ && sp);
  z_stream zs;
  zs.zalloc = Z_NULL;
  zs.zfree = Z_NULL;
  zs.opaque = Z_NULL;
  switch (mode) {
    default: {
      if (deflateInit2(&zs, 6, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY) != Z_OK) return NULL;
      break;
    }
    case DEFLATE: {
      if (deflateInit2(&zs, 6, Z_DEFLATED, 15, 9, Z_DEFAULT_STRATEGY) != Z_OK) return NULL;
      break;
    }
    case GZIP: {
      if (deflateInit2(&zs, 6, Z_DEFLATED, 15 + 16, 9, Z_DEFAULT_STRATEGY) != Z_OK) return NULL;
      break;
    }
  }
  const char* rp = (const char*)buf;
  size_t zsiz = size + size / 8 + 32;
  char* zbuf = new char[zsiz+1];
  char* wp = zbuf;
  zs.next_in = (Bytef*)rp;
  zs.avail_in = size;
  zs.next_out = (Bytef*)wp;
  zs.avail_out = zsiz;
  if (deflate(&zs, Z_FINISH) != Z_STREAM_END) {
    delete[] zbuf;
    deflateEnd(&zs);
    return NULL;
  }
  deflateEnd(&zs);
  zsiz -= zs.avail_out;
  zbuf[zsiz] = '\0';
  if (mode == RAW) zsiz++;
  *sp = zsiz;
  return zbuf;
#else
  _assert_(buf && size <= MEMMAXSIZ && sp);
  char* zbuf = new char[size+2];
  char* wp = zbuf;
  *(wp++) = 'z';
  *(wp++) = (uint8_t)mode;
  std::memcpy(wp, buf, size);
  *sp = size + 2;
  return zbuf;
#endif
}


/**
 * Decompress a serial data.
 */
char* Zlib::decompress(const void* buf, size_t size, size_t* sp, Mode mode) {
#if defined(_KC_ZLIB)
  _assert_(buf && size <= MEMMAXSIZ && sp);
  size_t zsiz = size * 8 + 32;
  while (true) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    switch (mode) {
      default: {
        if (inflateInit2(&zs, -15) != Z_OK) return NULL;
        break;
      }
      case DEFLATE: {
        if (inflateInit2(&zs, 15) != Z_OK) return NULL;
        break;
      }
      case GZIP: {
        if (inflateInit2(&zs, 15 + 16) != Z_OK) return NULL;
        break;
      }
    }
    char* zbuf = new char[zsiz+1];
    zs.next_in = (Bytef*)buf;
    zs.avail_in = size;
    zs.next_out = (Bytef*)zbuf;
    zs.avail_out = zsiz;
    int32_t rv = inflate(&zs, Z_FINISH);
    inflateEnd(&zs);
    if (rv == Z_STREAM_END) {
      zsiz -= zs.avail_out;
      zbuf[zsiz] = '\0';
      *sp = zsiz;
      return zbuf;
    } else if (rv == Z_BUF_ERROR) {
      delete[] zbuf;
      zsiz *= 2;
    } else {
      delete[] zbuf;
      break;
    }
  }
  return NULL;
#else
  _assert_(buf && size <= MEMMAXSIZ && sp);
  if (size < 2 || ((char*)buf)[0] != 'z' || ((char*)buf)[1] != (uint8_t)mode) return NULL;
  buf = (char*)buf + 2;
  size -= 2;
  char* zbuf = new char[size+1];
  std::memcpy(zbuf, buf, size);
  zbuf[size] = '\0';
  *sp = size;
  return zbuf;
#endif
}


/**
 * Calculate the CRC32 checksum of a serial data.
 */
uint32_t Zlib::calculate_crc(const void* buf, size_t size, uint32_t seed) {
#if defined(_KC_ZLIB)
  _assert_(buf && size <= MEMMAXSIZ);
  return crc32(seed, (unsigned char*)buf, size);
#else
  _assert_(buf && size <= MEMMAXSIZ);
  return 0;
#endif
}


/**
 * Prepared variable of the Zlib raw mode.
 */
ZlibCompressor<Zlib::RAW> ZLIBRAWCOMP;


}                                        // common namespace

// END OF FILE
