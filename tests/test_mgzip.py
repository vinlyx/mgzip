import pytest
import os
import mgzip
import gzip

DATA1 = b""""Beautiful is better than ugly.
Explicit is better than implicit.
Simple is better than complex.
Complex is better than complicated.
"""

def test_write_wb(tmpdir):

    filename = os.path.join(tmpdir, "test.gz")
    with mgzip.open(filename, 'wb', compresslevel=6) as f1:
        f1.write(DATA1 * 50)
        # Try flush and fileno.
        f1.flush()
        f1.fileno()
        if hasattr(os, 'fsync'):
            os.fsync(f1.fileno())
        f1.close()
    f1.close()

    assert os.path.exists(filename)
    with gzip.open(filename, 'rb') as f2:
        file_content = f2.read()
    assert file_content == DATA1 * 50


def test_read_rb(tmpdir):

    filename = os.path.join(tmpdir, "test.gz")
    with gzip.open(filename, 'wb') as f1:
        f1.write(DATA1 * 500)

    with mgzip.open(filename, 'rb') as f2:
        file_content = f2.read()
    assert file_content == DATA1 * 500


