'''
Randomly shuffles the lines of very large text files with minimal memory usage

__author__  = "Charles R. Schmidtc <schmidtc@gmail.com> "
'''

import random
import math
import tempfile

MAX_SHARD_SIZE_BYTES = 100*1024*1024.0 # 100 MB * 1024 (KB/MB) * 1024(B/KB)

def sharded_shuffle(in_name,out_name):
    global shards
    infile = open(in_name,'r')
    infile.seek(0,2)
    length = infile.tell()
    infile.seek(0)
    n_shards = int(math.ceil(length/MAX_SHARD_SIZE_BYTES))
    shards = [tempfile.TemporaryFile('w+') for i in range(n_shards)]

    print "sharding with %d shards"%n_shards
    for i,line in enumerate(infile):
        shard = i%n_shards
        shards[shard].write(line)
    infile.close()

    info = {}
    for i,shard in enumerate(shards):
        print "shuffling",i
        shard.flush()
        shard.seek(0)
        lines = shard.readlines()
        info[i] = len(lines)
        random.shuffle(lines)

        shard.seek(0)
        shard.truncate(0)
        shard.writelines(lines)
        shard.seek(0)

    o = open(out_name,'w')
    print "writing"
    while shards:
        shard = random.randrange(0,n_shards)
        line = shards[shard].readline()
        if line:
            o.write(line)
        else:
            print "closing shard"
            shards[shard].close()
            shards.pop(shard)
            n_shards = len(shards)
    o.close()
def print_usage():
    print "Usage: python largefile_shuffle.py /path/to/input.txt /path/to/output.txt"

if __name__=='__main__':
    import sys,os
    if len(sys.argv) == 3:
        i = sys.argv[1]
        o = sys.argv[2]
        if not os.path.exists(i):
            print_usage()
        else:
            sharded_shuffle(i,o)
    else:
        print_usage()
