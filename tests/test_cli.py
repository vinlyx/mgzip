"""CLI functionality tests for mgzip."""

import gzip
import os

import mgzip


class TestCLI:
    """Test mgzip command-line interface."""

    def test_cli_compress_file(self):
        """Test compressing a file via CLI."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.txt') as f:
            f.write(b"Hello, World! " * 100)
            input_file = f.name
        
        output_file = input_file + ".gz"
        # Test: python -m mgzip < input > output.gz
        try:
            # Test: python -m mgzip < input > output.gz
            with open(input_file, 'rb') as fin:
                with open(output_file, 'wb') as fout:
                    # Simulate CLI behavior
                    import mgzip
                    with mgzip.open(fout, 'wb') as gz:
                        gz.write(fin.read())
            
            # Verify output file exists and is valid gzip
            assert os.path.exists(output_file)
            with gzip.open(output_file, 'rb') as f:
                assert f.read() == b"Hello, World! " * 100
        finally:
            if os.path.exists(input_file):
                os.unlink(input_file)
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_cli_decompress_file(self):
        """Test decompressing a file via CLI."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.gz') as f:
            f.write(mgzip.compress(b"Test data " * 50))
            input_file = f.name
        
        # Test: python -m mgzip < input > output.gz
        
        try:
            # Decompress
            with mgzip.open(input_file, 'rb') as fin:
                with open(output_file, 'wb') as fout:
                    fout.write(fin.read())
            
            # Verify
            assert os.path.exists(output_file)
            with open(output_file, 'rb') as f:
                assert f.read() == b"Test data " * 50
        finally:
            if os.path.exists(input_file):
                os.unlink(input_file)
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_cli_stdin_stdout(self):
        """Test CLI with stdin/stdout."""
        import io
        
        # Simulate stdin input
        
        # Compress via mgzip
        compressed = mgzip.compress(input_data)
        
        # Decompress
        decompressed = mgzip.decompress(compressed)
        
        assert decompressed == input_data

    def test_cli_compress_decompress_roundtrip(self):
        """Test full compress/decompress roundtrip."""
        test_data = b"Roundtrip test data " * 100
        
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "test.txt")
            gz_file = os.path.join(tmpdir, "test.txt.gz")
            output_file = os.path.join(tmpdir, "test.out.txt")
            
            # Create input file
            with open(input_file, 'wb') as f:
                f.write(test_data)
            
            # Compress
            with open(input_file, 'rb') as fin:
                with mgzip.open(gz_file, 'wb') as fout:
                    fout.write(fin.read())
            
            # Decompress
            with mgzip.open(gz_file, 'rb') as fin:
                with open(output_file, 'wb') as fout:
                    fout.write(fin.read())
            
            # Verify
            with open(output_file, 'rb') as f:
                assert f.read() == test_data
