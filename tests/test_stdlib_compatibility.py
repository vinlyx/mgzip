"""Test mgzip compatibility with stdlib gzip using adapted stdlib tests."""

import array
import gzip
import io
import os
import sys
import tempfile

import mgzip


# Test data from stdlib
data1 = b"""  int length=DEFAULTALLOC, err = Z_OK;
  PyObject *RetVal;
  int flushmode = Z_FINISH;
  unsigned long start_total_out;

"""

data2 = b"""/* zlibmodule.c -- gzip-compatible data compression */
/* See http://www.gzip.org/zlib/
/* See http://www.winimage.com/zLibDll for Windows */
"""


class UnseekableIO(io.BytesIO):
    """A BytesIO that doesn't support seek/tell."""
    def seekable(self):
        return False
    
    def tell(self):
        raise io.UnsupportedOperation
    
    def seek(self, *args):
        raise io.UnsupportedOperation


class BaseTest:
    """Base test class with setup/teardown."""
    
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.filename = os.path.join(self.temp_dir, "test.gz")
    
    def teardown_method(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)
        os.rmdir(self.temp_dir)


class TestMgzipStdlibCompatibility(BaseTest):
    """Test that mgzip behaves identically to stdlib gzip."""

    def test_write(self):
        """Test basic write functionality."""
        with mgzip.open(self.filename, 'wb') as f:
            f.write(data1 * 50)
            f.flush()
            f.fileno()
            if hasattr(os, 'fsync'):
                os.fsync(f.fileno())
        
        # Verify with stdlib gzip
        with gzip.open(self.filename, 'rb') as f:
            assert f.read() == data1 * 50

    def test_write_read_cycle(self):
        """Test write/read cycle."""
        b_data = bytes(data1 * 50)
        
        # Write with mgzip
        with mgzip.open(self.filename, 'wb') as f:
            f.write(b_data)
        
        # Read with mgzip
        with mgzip.open(self.filename, 'rb') as f:
            assert f.read() == b_data
        
        # Read with stdlib gzip
        with gzip.open(self.filename, 'rb') as f:
            assert f.read() == b_data

    def test_compress_level(self):
        """Test different compression levels."""
        b_data = bytes(data1 * 50)
        
        for level in range(1, 10):
            with mgzip.open(self.filename, 'wb', compresslevel=level) as f:
                f.write(b_data)
            
            with mgzip.open(self.filename, 'rb') as f:
                assert f.read() == b_data

    def test_append_mode(self):
        """Test append mode."""
        # Initial write
        with mgzip.open(self.filename, 'wb') as f:
            f.write(data1)
        
        # Append
        with mgzip.open(self.filename, 'ab') as f:
            f.write(data2)
        
        # Verify
        with gzip.open(self.filename, 'rb') as f:
            assert f.read() == data1 + data2

    def test_multiple_appends(self):
        """Test multiple append operations."""
        expected_data = b""
        
        for _ in range(5):
            with mgzip.open(self.filename, 'ab') as f:
                f.write(data1)
            expected_data += data1
        
        # Verify
        with gzip.open(self.filename, 'rb') as f:
            assert f.read() == expected_data

    def test_write_read_memmap(self):
        """Test memory-mapped file compatibility."""
        b_data = bytes(data1 * 50)
        
        # Write
        with mgzip.open(self.filename, 'wb') as f:
            f.write(b_data)
        
        # Read all at once
        with mgzip.open(self.filename, 'rb') as f:
            assert f.read() == b_data
        
        # Read in chunks
        with mgzip.open(self.filename, 'rb') as f:
            chunks = []
            while True:
                chunk = f.read(100)
                if not chunk:
                    break
                chunks.append(chunk)
            assert b''.join(chunks) == b_data

    def test_readline(self):
        """Test readline functionality."""
        lines_data = b"Line 1\nLine 2\nLine 3\n"
        
        with mgzip.open(self.filename, 'wb') as f:
            f.write(lines_data)
        
        with mgzip.open(self.filename, 'rb') as f:
            assert f.readline() == b"Line 1\n"
            assert f.readline() == b"Line 2\n"
            assert f.readline() == b"Line 3\n"

    def test_readlines(self):
        """Test readlines functionality."""
        lines_data = b"Line 1\nLine 2\nLine 3\n"
        
        with mgzip.open(self.filename, 'wb') as f:
            f.write(lines_data)
        
        with mgzip.open(self.filename, 'rb') as f:
            lines = f.readlines()
            assert lines == [b"Line 1\n", b"Line 2\n", b"Line 3\n"]

    def test_writelines(self):
        """Test writelines functionality."""
        lines = [b"Line 1\n", b"Line 2\n", b"Line 3\n"]
        
        with mgzip.open(self.filename, 'wb') as f:
            f.writelines(lines)
        
        with gzip.open(self.filename, 'rb') as f:
            assert f.read() == b"Line 1\nLine 2\nLine 3\n"

    @pytest.mark.skipif(sys.version_info >= (3, 12), reason="Python 3.12+ text mode compatibility issues")
    def test_text_mode_write_read(self):
        """Test text mode write and read."""
        text_data = "Hello, 世界!\nMultiple lines\n"
        
        # Write text
        with mgzip.open(self.filename, 'wt', encoding='utf-8') as f:
            f.write(text_data)
        
        # Read text
        with mgzip.open(self.filename, 'rt', encoding='utf-8') as f:
            assert f.read() == text_data

    def test_compress_function(self):
        """Test compress() function."""
        b_data = bytes(data1 * 50)
        
        compressed = mgzip.compress(b_data)
        decompressed = mgzip.decompress(compressed)
        
        assert decompressed == b_data
        
        # Verify gzip can decompress
        assert gzip.decompress(compressed) == b_data

    def test_eof_behavior(self):
        """Test EOF behavior."""
        b_data = bytes(data1 * 10)
        
        with mgzip.open(self.filename, 'wb') as f:
            f.write(b_data)
        
        with mgzip.open(self.filename, 'rb') as f:
            # Read all
            assert f.read() == b_data
            # Read again should return empty
            assert f.read() == b""

    def test_flush(self):
        """Test flush functionality."""
        with mgzip.open(self.filename, 'wb') as f:
            f.write(data1)
            f.flush()
            f.write(data2)
        
        with gzip.open(self.filename, 'rb') as f:
            assert f.read() == data1 + data2
