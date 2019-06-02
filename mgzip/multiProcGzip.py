"""This module provide a simple replacement of Python internal gzip module
to provide a multiprocessing solution for gzip compression/decompression.

License: MIT LICENSE
Copyright (c) 2019 Vincent Li

"""

import os, time
import struct
import zlib
from gzip import GzipFile, write32u, READ, WRITE, FEXTRA, FNAME, FCOMMENT
from multiprocessing.dummy import Pool

__version__ = "0.1.0"

class RandomAccessGzipFile(GzipFile):
    """ docstring of RandomAccessGzipFile """

    INIT_ZEROS = b'\x00' * 16

    def __init__(self, filename=None, mode=None,
                 compresslevel=9, fileobj=None, mtime=None,
                 thread=None, blocksize=10**7):
        super().__init__(filename=filename, mode=mode, compresslevel=compresslevel,
                                             fileobj=fileobj, mtime=mtime)

        self.thread = thread
        self.blocksize = blocksize # use 10M blocksize as default
        if not self.thread:
            ## thread is None or 0, use all available CPUs
            cpuNum = os.cpu_count()
            self.thread = cpuNum

    def _write_gzip_header(self):
        self.startIdx = self.fileobj.tell()         # save start index for append mode
        print("MemberStart", self.startIdx)
        self.fileobj.write(b'\037\213')             # magic header
        self.fileobj.write(b'\010')                 # compression method
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
        self.fileobj.write(chr(flags).encode('latin-1'))
        mtime = self._write_mtime
        if mtime is None:
            mtime = time.time()
        write32u(self.fileobj, int(mtime))
        self.fileobj.write(b'\002')
        self.fileobj.write(b'\377')
        # write extra flag for indexing
        # XLEN, 20 bytes
        self.fileobj.write(b'\x14\x00')
        # EXTRA FLAG FORMAT:
        # +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
        # |SI1|SI2|  LEN  |       OFFSET (8 Bytes)        |     ORIGINAL SIZE (8 Bytes)   |
        # +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
        # SI1: 'I'  SI2: 'G' (Indexed Gzip file)
        self.fileobj.write(b'IG')
        # LEN: 16 bytes
        self.fileobj.write(b'\x10\x00')
        # fill zero
        print("FEXTRAOffset", self.fileobj.tell())
        self.fileobj.write(self.INIT_ZEROS)
        if fname:
            self.fileobj.write(fname + b'\000')

    def close(self):
        fileobj = self.fileobj
        if fileobj is None:
            return
        self.fileobj = None
        try:
            if self.mode == WRITE:
                fileobj.write(self.compress.flush())
                write32u(fileobj, self.crc)
                # self.size may exceed 2GB, or even 4GB
                write32u(fileobj, self.size & 0xffffffff)
                currOffset = fileobj.tell()
                print("memberEnd", currOffset)
                # FEXTRA Offset is 16 bytes shifted from start position
                fileobj.seek(self.startIdx + 16)
                print("???", fileobj.tell())
                fileobj.write(struct.pack("<Q", currOffset - self.startIdx))
                fileobj.write(struct.pack("<Q", self.size))
                print("???End", fileobj.tell())
                fileobj.seek(currOffset)
                # fileobj.seek()
                print("fileEnd", fileobj.tell())
            elif self.mode == READ:
                self._buffer.close()
        finally:
            myfileobj = self.myfileobj
            if myfileobj:
                self.myfileobj = None
                myfileobj.close()