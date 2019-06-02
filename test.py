import mgzip

with mgzip.open("test.txt.gz", "at") as fh:
    fh.write("Hello World~~~!\n")

