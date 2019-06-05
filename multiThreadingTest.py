import time
import random
import zlib
from multiprocessing.dummy import Pool, Queue



class T1(object):
    """ docstring of T1 """

    def __init__(self, data):
        # super().__init__()
        self.data = data
        self.p = Pool(7)

    def _compressFunc(self, data, i):
        cpr = zlib.compressobj(9,
                               zlib.DEFLATED,
                               -zlib.MAX_WBITS,
                               zlib.DEF_MEM_LEVEL,
                               0)
        bodyBytes = cpr.compress(data)
        resBytes = cpr.flush(zlib.Z_SYNC_FLUSH)
        crc = zlib.crc32(data)
        print("finished: {:d}".format(i))
        return (bodyBytes, resBytes, crc, len(data))

    def compress(self, data):
        # rlt = data[:30]
        print("running sleep: {:d}".format(data))
        time.sleep(data)
        print("sleep: {:d}".format(data))
        return id(data)

    def start(self):
        for i in range(1, 2):
            print("sended: {:d}".format(i))
            # rtn = self.p.apply_async(self._compressFunc, args=(self.data, i))
            # rtn = self.p.apply_async(self.compress, args=(i,))
            rtn = self._compressFunc(self.data, i)
            # print(rtn)

    def fin(self):
        self.p.close()
        self.p.join()

# tsize = int(1.2 * 10 ** 9)
tsize = 1 * 10 ** 8
with open("zipTest", 'rb', tsize) as fh:
    rawData = fh.read(tsize)

t = T1(rawData)

t.start()
t.fin()




