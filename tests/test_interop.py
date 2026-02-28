"""Interoperability tests with stdlib gzip."""

import gzip
import io
import os
import sys
import tempfile

import pytest

import mgzip


# Test data
DATA1 = b"""  int length=DEFAULTALLOC, err = Z_OK;
  PyObject *RetVal;
  int flushmode = Z_FINISH;
  unsigned long start_total_out;

"""

DATA2 = b"""/* zlibmodule.c -- gzip-compatible data compression */
/* See http://www.gzip.org/zlib/
"""


class TestMgzipGzipCompatibility:
    """Test mgzip compatibility with stdlib gzip."""

    def test_write_read_cycle(self):
        """Test that mgzip files can be read by gzip and vice versa."""
        test_data = DATA1 * 50
        
        with tempfile.NamedTemporaryFile(suffix='.gz', delete=False) as f:
            fname = f.name
        
        try:
            # Write with mgzip, read with gzip
            with mgzip.open(fname, 'wb') as f:
                f.write(test_data)
            
            with gzip.open(fname, 'rb') as f:
                assert f.read() == test_data
            
            # Write with gzip, read with mgzip (Python < 3.12)
            if sys.version_info < (3, 12):
                with gzip.open(fname, 'wb') as f:
                    f.write(test_data)
                
                with mgzip.open(fname, 'rb') as f:
                    assert f.read() == test_data
        finally:
            if os.path.exists(fname):
                os.unlink(fname)

    @pytest.mark.skipif(sys.version_info >= (3, 12), reason="Python 3.12+ text mode compatibility issues")
    def test_text_mode(self):
        """Test text mode compatibility."""
        text_data = "Hello, 世界!\nMultiple lines\nWith unicode"
        
        with tempfile.NamedTemporaryFile(suffix='.gz', delete=False, mode='w') as f:
            fname = f.name
        
        try:
            # Write with mgzip text mode
            with mgzip.open(fname, 'wt', encoding='utf-8') as f:
                f.write(text_data)
            
            # Read with gzip text mode
            with gzip.open(fname, 'rt', encoding='utf-8') as f:
                assert f.read() == text_data
        finally:
            if os.path.exists(fname):
                os.unlink(fname)

    def test_compress_decompress_functions(self):
        """Test compress() and decompress() compatibility."""
        test_data = DATA1 * 100
        
        # mgzip.compress -> gzip.decompress
        mgzip_compressed = mgzip.compress(test_data)
        assert gzip.decompress(mgzip_compressed) == test_data
        
        # gzip.compress -> mgzip.decompress
        gzip_compressed = gzip.compress(test_data)
        assert mgzip.decompress(gzip_compressed) == test_data

    def test_file_compatibility(self):
        """Test file format compatibility."""
        test_data = b"File format test " * 50
        
        with tempfile.NamedTemporaryFile(suffix='.gz', delete=False) as f:
            fname = f.name
        
        try:
            # Write with mgzip
            with mgzip.open(fname, 'wb') as f:
                f.write(test_data)
            
            # Verify it's a valid gzip file (check magic number)
            with open(fname, 'rb') as f:
                magic = f.read(2)
                assert magic == b'\x1f\x8b', "Not a valid gzip file"
            
            # Read with stdlib gzip
            with gzip.open(fname, 'rb') as f:
                assert f.read() == test_data
        finally:
            if os.path.exists(fname):
                os.unlink(fname)
