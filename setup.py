from setuptools import setup, find_packages
from mgzip import __version__

with open('README.md') as fh:
    longDesc = fh.read().replace("CompressionBenchmark.png", "https://raw.githubusercontent.com/vinlyx/mgzip/master/CompressionBenchmark.png").replace("DecompressionBenchmark.png", "https://raw.githubusercontent.com/vinlyx/mgzip/master/DecompressionBenchmark.png")

setup(
    name='mgzip',
    version=__version__,
    author='Craig Milo Rogers',
    author_email='CraigMiloRogers@gmail.com',

    description='A multi-threading implementation of Python gzip module, based on the work of Vincent Li.',
    long_description=longDesc,
    long_description_content_type="text/markdown",
    url='https://github.com/usc-isi-i2/mgzip',
    license='MIT',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
    ],
    python_requires=">=3.6"
)
