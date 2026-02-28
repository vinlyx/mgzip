"""Basic functionality tests for mgzip"""
import pytest
import mgzip
import gzip
import tempfile
import os
import sys


class TestBasicCompressDecompress:
    """Test basic compression and decompression functionality"""

    def test_compress_decompress_simple(self):
        """Test simple compress and decompress round-trip"""
        data = b"Hello, World! " * 1000
        compressed = mgzip.compress(data)
        decompressed = mgzip.decompress(compressed)
        assert decompressed == data

    def test_compress_decompress_empty(self):
        """Test compress and decompress empty data"""
        data = b""
        compressed = mgzip.compress(data)
        decompressed = mgzip.decompress(compressed)
        assert decompressed == data

    def test_compress_decompress_large(self):
        """Test compress and decompress large data"""
        data = b"X" * 1000000  # 1MB
        compressed = mgzip.compress(data)
        decompressed = mgzip.decompress(compressed)
        assert decompressed == data
        # Check compression actually happened
        assert len(compressed) < len(data)


class TestFileOperations:
    """Test file read/write operations"""

    def test_file_write_read(self):
        """Test writing to and reading from file"""
        data = b"Test data " * 100
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            # Write
            with mgzip.open(fname, "wb") as f:
                f.write(data)
            
            # Read
            with mgzip.open(fname, "rb") as f:
                result = f.read()
            
            assert result == data
        finally:
            os.unlink(fname)

    def test_file_context_manager(self):
        """Test context manager properly closes files"""
        data = b"Context manager test"
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            with mgzip.open(fname, "wb") as f:
                f.write(data)
            
            with mgzip.open(fname, "rb") as f:
                result = f.read()
            
            assert result == data
        finally:
            os.unlink(fname)

    def test_file_modes(self):
        """Test different file modes"""
        data = b"Binary mode test"
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            # Binary write
            with mgzip.open(fname, "wb") as f:
                f.write(data)
            
            # Binary read
            with mgzip.open(fname, "rb") as f:
                result = f.read()
            
            assert result == data
        finally:
            os.unlink(fname)


class TestCompatibility:
    """Test compatibility with standard gzip module"""

    @pytest.mark.skipif(sys.version_info >= (3, 12), reason="Python 3.12+ has zlib API changes")
    def test_mgzip_read_gzip_write(self):
        """Test mgzip can read files written by gzip"""
        data = b"Compatibility test data"
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            # Write with standard gzip
            with gzip.open(fname, "wb") as f:
                f.write(data)
            
            # Read with mgzip
            with mgzip.open(fname, "rb") as f:
                result = f.read()
            
            assert result == data
        finally:
            os.unlink(fname)

    def test_gzip_read_mgzip_write(self):
        """Test gzip can read files written by mgzip"""
        data = b"Compatibility test data"
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            # Write with mgzip
            with mgzip.open(fname, "wb") as f:
                f.write(data)
            
            # Read with standard gzip
            with gzip.open(fname, "rb") as f:
                result = f.read()
            
            assert result == data
        finally:
            os.unlink(fname)


class TestMultiprocessingPool:
    """Test that multiprocessing pool is properly closed (Issue #13, PR #18)"""

    @pytest.mark.skipif(sys.version_info >= (3, 12), reason="Python 3.12+ has different warning behavior")
    def test_pool_cleanup_no_warning(self):
        """Test that closing file doesn't leave unclosed pool warnings"""
        import warnings
        
        data = b"Pool cleanup test" * 1000
        
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            # Capture warnings
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                
                with mgzip.open(fname, "wb", thread=4) as f:
                    f.write(data)
                
                # Force garbage collection
                import gc
                gc.collect()
                
                # Check for ResourceWarning about unclosed pool
                resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
                # Should have no unclosed pool warnings
                pool_warnings = [x for x in resource_warnings if "pool" in str(x.message).lower()]
                assert len(pool_warnings) == 0, f"Found unclosed pool warnings: {pool_warnings}"
        finally:
            os.unlink(fname)

    def test_file_operations_no_crash(self):
        """Test that file operations complete without crashing"""
        data = b"Test data" * 1000
        
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            fname = f.name
        
        try:
            # Write and read should complete without errors
            with mgzip.open(fname, "wb", thread=4) as f:
                f.write(data)
            
            with mgzip.open(fname, "rb", thread=4) as f:
                result = f.read()
            
            assert result == data
        finally:
            os.unlink(fname)
