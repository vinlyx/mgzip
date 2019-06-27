"""This module provide a simple replacement of Python internal gzip module
to provide a multiprocessing solution for gzip compression/decompression.

License: MIT LICENSE
Copyright (c) 2019 Vincent Li

"""

import gzip as gzlib
from .multiProcGzip import MulitGzipFile, _MulitGzipReader, __version__

__all__ = ["GzipFile", "open", "compress", "decompress"]


## patch GzipFile with ParallelGzipFile
gzlib._GzipReader = _MulitGzipReader
gzlib.GzipFile = MulitGzipFile

## original methods
GzipFile = MulitGzipFile
open = gzlib.open
compress = gzlib.compress
decompress = gzlib.decompress