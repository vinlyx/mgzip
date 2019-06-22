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
        pass
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
            speed = size/(t1 - t0)
            seconds = t1 - t0
            nsize = os.stat(outf).st_size
            print("Compressed {:.2f} MB data in {:.2f} S, Speed: {:.2f} MB/s, Rate: {:.2f} %".format(size, seconds, speed, nsize/len(data)*100))

if __name__ == '__main__':
    _test()