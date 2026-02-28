"""A multi-threading implementation of Python gzip module.

License: MIT LICENSE
Copyright (c) 2019 Vincent Li
"""

from .multiProcGzip import (
    MultiGzipFile,
    open,
    compress,
    decompress,
)

__version__ = None  # Imported dynamically by setuptools

__all__ = ["GzipFile", "open", "compress", "decompress", "__version__"]

GzipFile = MultiGzipFile
