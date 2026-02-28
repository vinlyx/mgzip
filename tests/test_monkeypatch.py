"""Run stdlib gzip tests against mgzip by monkey-patching."""

import gzip
import os
import tempfile

import pytest

import mgzip


class StdlibTestRunner:
    """Run stdlib gzip tests against mgzip implementation."""

    def __init__(self):
        self.original_gzip = None

    def patch_gzip_module(self):
        """Replace gzip module functions with mgzip equivalents."""
        self.original_gzip = {
            "open": gzip.open,
            "GzipFile": gzip.GzipFile,
            "compress": gzip.compress,
            "decompress": gzip.decompress,
        }

        # Monkey patch gzip module
        gzip.open = mgzip.open
        gzip.GzipFile = mgzip.MultiGzipFile
        gzip.compress = mgzip.compress
        gzip.decompress = mgzip.decompress

    def restore_gzip_module(self):
        """Restore original gzip module."""
        if self.original_gzip:
            gzip.open = self.original_gzip["open"]
            gzip.GzipFile = self.original_gzip["GzipFile"]
            gzip.compress = self.original_gzip["compress"]
            gzip.decompress = self.original_gzip["decompress"]


# Test data from stdlib
data1 = b"""  int length=DEFAULTALLOC, err = Z_OK;
  PyObject *RetVal;
  int flushmode = Z_FINISH;
  unsigned long start_total_out;

"""


@pytest.fixture
def monkey_patched_gzip():
    """Fixture to monkey patch gzip module for tests."""
    runner = StdlibTestRunner()
    runner.patch_gzip_module()
    yield
    runner.restore_gzip_module()


@pytest.fixture
def temp_file():
    """Create a temporary file for testing."""
    fd, path = tempfile.mkstemp(suffix=".gz")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


class TestMonkeyPatchedGzip:
    """Test that mgzip can replace gzip transparently."""

    def test_compress_decompress(self, monkey_patched_gzip):
        """Test that monkey-patched compress/decompress works."""
        test_data = data1 * 50
        compressed = gzip.compress(test_data)
        decompressed = gzip.decompress(compressed)
        assert decompressed == test_data

    def test_file_operations(self, temp_file, monkey_patched_gzip):
        """Test that monkey-patched file operations work."""
        test_data = data1 * 50

        # Write with monkey-patched gzip
        with gzip.open(temp_file, "wb") as f:
            f.write(test_data)

        # Read with monkey-patched gzip
        with gzip.open(temp_file, "rb") as f:
            assert f.read() == test_data

    def test_context_manager(self, temp_file, monkey_patched_gzip):
        """Test that monkey-patched context manager works."""
        test_data = data1 * 10

        with gzip.open(temp_file, "wb") as f:
            f.write(test_data)

        with gzip.open(temp_file, "rb") as f:
            result = f.read()

        assert result == test_data

    def test_multiple_files(self, monkey_patched_gzip):
        """Test multiple concurrent file operations."""
        files = []
        try:
            # Create multiple files
            for i in range(3):
                fd, path = tempfile.mkstemp(suffix=".gz")
                os.close(fd)
                files.append(path)

                test_data = data1 * (i + 1) * 10

                # Write
                with gzip.open(path, "wb") as f:
                    f.write(test_data)

            # Read and verify
            for i, path in enumerate(files):
                test_data = data1 * (i + 1) * 10
                with gzip.open(path, "rb") as f:
                    assert f.read() == test_data
        finally:
            # Cleanup
            for path in files:
                if os.path.exists(path):
                    os.unlink(path)
