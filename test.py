import mgzip
# import gzip as mgzip
import time

def _test():
    import sys
    import os
    # Act like gzip; with -d, act like gunzip.
    # The input file is not deleted, however, nor are any other gzip
    # options or features supported.
    args = sys.argv[1:]
    decompress = args and args[0] == "-d"
    if decompress:
        arg = args[1]
    else:
        arg = args[0]
    # if not args:
    #     args = ["-"]
    if decompress:
        if arg != "-":
            outf = arg + ".dcp"
            outf = "/dev/null"
            fh = open(outf, "wb")
            gh = mgzip.open(arg, "rb", 100*10**6)
            t0 = time.time()
            data = gh.read()
            t1 = time.time()
            fh.write(data)
            fh.close()
            gh.close()
            size = len(data)/(1024**2)
            seconds = t1 - t0
            speed = size/seconds
            nsize = os.stat(arg).st_size
            print("Decompressed {:.2f} MB data in {:.2f} S, Speed: {:.2f} MB/s, Rate: {:.2f} %".format(size, seconds, speed, nsize/len(data)*100))
    else:
        if arg != "-":
            outf = arg + ".gz"
            fh = open(arg, "rb")
            gh = mgzip.open(outf, "wb", compresslevel=6)
            data = fh.read()
            t0 = time.time()
            gh.write(data)
            gh.close()
            t1 = time.time()
            size = len(data)/(1024**2)
            seconds = t1 - t0
            speed = size/seconds
            nsize = os.stat(outf).st_size
            print("Compressed {:.2f} MB data in {:.2f} S, Speed: {:.2f} MB/s, Rate: {:.2f} %".format(size, seconds, speed, nsize/len(data)*100))

if __name__ == '__main__':
    _test()