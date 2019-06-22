"""This module provide a simple replacement of Python internal gzip module
to provide a multiprocessing solution for gzip compression/decompression.

License: MIT LICENSE
Copyright (c) 2019 Vincent Li

"""

import os, time
import builtins
import struct
import zlib
from io import BytesIO
from gzip import GzipFile, write32u, READ, WRITE, FEXTRA, FNAME, FCOMMENT
from multiprocessing.dummy import Pool

__version__ = "0.1.0"

class MulitGzipFile(GzipFile):
    """ docstring of MulitGzipFile """

    # INIT_ZEROS = b'\x00' * 16

    def __init__(self, filename=None, mode=None,
                 compresslevel=9, fileobj=None, mtime=None,
                 thread=None, blocksize=5*10**7):

        if mode and ('t' in mode or 'U' in mode):
            raise ValueError("Invalid mode: {!r}".format(mode))
        if mode and 'b' not in mode:
            mode += 'b'
        if mode and "a" in mode:
            # fix append mode
            if filename and os.path.exists(filename):
                # if append to exists file, use 'r+' mode
                if "+" in mode:
                    # a+ --> r+
                    fixMode = mode.replace("a", "r")
                else:
                    # a --> r+
                    fixMode = mode.replace("a", "r+")
            else:
                # a --> w
                fixMode = mode.replace("a", "w")
        else:
            fixMode = mode
        if fileobj is None:
            fileobj = self.myfileobj = builtins.open(filename, fixMode or 'rb')
        if mode and "a" in mode:
            # move to the end for append mode
            fileobj.seek(0, 2)

        super().__init__(filename=filename, mode=mode, compresslevel=compresslevel,
                                             fileobj=fileobj, mtime=mtime)

        self.compresslevel = compresslevel
        self.thread = thread
        self.blocksize = blocksize # use 20M blocksize as default
        if not self.thread:
            ## thread is None or 0, use all available CPUs
            self.thread = os.cpu_count()
        self.pool = Pool(self.thread)
        self.pool_result = []
        self.small_buf = BytesIO()
        # self.buffSize = 0

    def _write_gzip_header(self):
        ## ignored to write original header
        pass

    def _compress_func(self, data, pdata=None):
        """
            Compress data with zlib deflate algorithm.
            Input:
                data: btyes object of input data
                pdata: exists small buffer data
            Return:
                tuple of (Buffered compressed data,
                          Major compressed data,
                          Rest data after flush buffer,
                          CRC32,
                          Original size)
        """
        cpr = zlib.compressobj(self.compresslevel,
                               zlib.DEFLATED,
                               -zlib.MAX_WBITS,
                               9, # use memory level 9 > zlib.DEF_MEM_LEVEL (8) for better performance
                               0)
        if pdata:
            prefix_bytes = cpr.compress(pdata)
        body_bytes = cpr.compress(data)
        rest_bytes = cpr.flush()
        if pdata:
            crc = zlib.crc32(data, zlib.crc32(pdata))
            return (prefix_bytes, body_bytes, rest_bytes, crc, pdata.nbytes + data.nbytes)
        else:
            crc = zlib.crc32(data)
            return (b'', body_bytes, rest_bytes, crc, data.nbytes)

    def write(self, data):
        self._check_not_closed()
        if self.mode != WRITE:
            import errno
            raise OSError(errno.EBADF, "write() on read-only GzipFile object")

        if self.fileobj is None:
            raise ValueError("write() on closed GzipFile object")

        data = memoryview(data)
        length = data.nbytes

        if length == 0:
            return length
        elif length >= self.blocksize:
            if length < 2 * self.blocksize:
                # use sigle thread
                self._compress_block_async(data)
            else:
                for st in range(0, length, self.blocksize):
                    self._compress_block_async(data[st: st+self.blocksize])
                    self._flush_pool()
        elif length < self.blocksize:
            self.small_buf.write(data)
            if self.small_buf.tell() >= self.blocksize:
                self._compress_async(self.small_buf.getbuffer())
                self.small_buf = BytesIO()
        self._flush_pool()
        return length

    def _compress_async(self, data, pdata=None):
        return self.pool_result.append(self.pool.apply_async(self._compress_func, args=(data, pdata)))

    def _compress_block_async(self, data):
        if self.small_buf.tell() != 0:
            self._compress_async(data, self.small_buf.getbuffer())
            self.small_buf = BytesIO()
        else:
            self._compress_async(data)

    def _flush_pool(self, force=False):
        if len(self.pool_result) <= self.thread and not force:
            return 0
        length = 0
        if force:
            flushSize = len(self.pool_result)
        else:
            flushSize = len(self.pool_result) - self.thread
        for i in range(flushSize):
            cdata = self.pool_result.pop(0).get()
            length += self._write_member(cdata)
            # (bodyBytes, resBytes, crc, oriSize) = rlt.get()
            # compressRlt = rlt.get()
        return length

    def _write_member(self, cdata):
        """
            Write a compressed data as a complete gzip member
            Input:
                cdata:
                    compressed data, a tuple of compressed result returned by _compress_func()
            Return:
                size of member
        """
        size = self._write_member_header(len(cdata[0]) + len(cdata[1]) + len(cdata[2]), cdata[4])
        self.fileobj.write(cdata[0])                   # buffer data
        self.fileobj.write(cdata[1])                   # body data
        self.fileobj.write(cdata[2])                   # rest data
        write32u(self.fileobj, cdata[3])               # CRC32
        write32u(self.fileobj, cdata[4] & 0xffffffff)  # raw data size in 32bits
        return size

    def _write_member_header(self, compressed_size, raw_size):
        self.fileobj.write(b'\037\213')             # magic header, 2 bytes
        self.fileobj.write(b'\010')                 # compression method, 1 byte
        try:
            # RFC 1952 requires the FNAME field to be Latin-1. Do not
            # include filenames that cannot be represented that way.
            fname = os.path.basename(self.name)
            if not isinstance(fname, bytes):
                fname = fname.encode('latin-1')
            if fname.endswith(b'.gz'):
                fname = fname[:-3]
        except UnicodeEncodeError:
            fname = b''
        flags = FEXTRA
        if fname:
            flags |= FNAME
        self.fileobj.write(chr(flags).encode('latin-1'))  # flags, 1 byte
        mtime = self._write_mtime
        if mtime is None:
            mtime = time.time()
        write32u(self.fileobj, int(mtime))          # modified time, 4 bytes
        self.fileobj.write(b'\002')                 # fixed flag (maximum compression), 1 byte
        self.fileobj.write(b'\377')                 # OS (unknown), 1 byte

        # write extra flag for indexing
        # XLEN, 20 bytes
        self.fileobj.write(b'\x14\x00')             # extra flag len, 2 bytes
        # EXTRA FLAG FORMAT:
        # +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
        # |SI1|SI2|  LEN  |       MEMBER SIZE (8 Bytes)   |       RAW SIZE (8 Bytes)      |
        # +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
        # SI1, SI2:      Subfield ID, 'IG' (Indexed Gzip file)
        # LEN:           Length of subfield body, always 16 bits
        # MEMBER SIZE:   The size of current member
        # RAW SIZE:      Raw text size in uint64 (since raw size is not able to represent >4GB file)
        self.fileobj.write(b'IG')                   # subfield ID (IG), 2 bytes
        # LEN: 16 bytes
        self.fileobj.write(b'\x10\x00')             # subfield len (16), 2 bytes
        # compressed data size: 16 + 8 + 8 + len(fname) + 1 + data + 8
        member_size = 32 + len(fname) + 1 + compressed_size + 8
        self.fileobj.write(struct.pack("<Q", member_size)) # member size, 8 bytes
        # raw data size:
        self.fileobj.write(struct.pack("<Q", raw_size))    # raw data size, 8 bytes
        if fname:
            self.fileobj.write(fname + b'\000')
        return member_size

    def close(self):
        fileobj = self.fileobj
        if fileobj is None:
            return
        try:
            if self.mode == WRITE:
                if self.small_buf.tell() != 0:
                    self._compress_async(self.small_buf.getbuffer())
                    self.small_buf = BytesIO()
                self._flush_pool(force=True)
            elif self.mode == READ:
                self._buffer.close()
        finally:
            self.fileobj = None
            myfileobj = self.myfileobj
            if myfileobj:
                self.myfileobj = None
                myfileobj.close()

    def flush(self):
        self._flush_pool(force=True)
        self.fileobj.flush()
