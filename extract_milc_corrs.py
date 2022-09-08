#!/usr/bin/env python

# Andrew Lytle
# March 2015

import os
import re
import subprocess
import sys
from itertools import product
from multiprocessing import Pool
from time import time

import numpy as np

REGEX = re.compile('correlator_key:')  # Pattern signaling end of header.
#T = 48

def sort_argv(argv):
    "Sort arguments in *.*.cfg_tsrc_* by cfg then src."
    def sort_fun(arg):
        arg = os.path.basename(arg)  # Remove path.
        sarg = arg.split('_')
        cfg = int(sarg[0].split('.')[-1])
        src = int(sarg[1].lstrip('t'))
        return (cfg, src)

    return sorted(argv, key=sort_fun)

def remove_headers(lines, T):
    "Lines corresponding to correlation functions."
    result = []
    for i, line in enumerate(lines):
        if REGEX.match(line):
            # Skip "..." line after the match.
            result.append(lines[i+2:i+2+T])
    return result

def real_part(lines):
    "Real part of correlation functions."
    return [float(line.split()[1]) for line in lines]

def extract(fname, T):
    with open(fname, 'r') as f:
        lines = f.readlines()
    _corr_key = corr_key(lines)  # Assumes only 1 unique per file.
    lines = remove_headers(lines, T)
    nums = np.array(list(map(real_part, lines)))
    return _corr_key, np.average(nums, axis=0)  # I think this averages over time sources..

def corr_key(lines):
    """Extract the correlator key from milc output file. 
    
    e.g. correlator_key:     P5-P5...
    """
    for line in lines:
        if REGEX.match(line):
            return line.strip().split()[-1]

def get_dirs(loc):
    dirs = os.listdir(loc)
    return dirs

def traverse(base):
    for d in get_dirs(base):
        for d2 in get_dirs(base+'/'+d):
            #yield base+'/'+d+'/'+d2
            for f in get_dirs(base+'/'+d+'/'+d2):
                yield base+'/'+d+'/'+d2+'/'+f
    
def _write_all(base, loc_root, T):
    "Write all (loose) correlators corresponding to base."
    for dir in get_dirs(base):
        for dir2 in get_dirs(base+'/'+dir):
            for f in get_dirs(base+'/'+dir+'/'+dir2):
                loc = base+'/'+dir+'/'+dir2
                corr_key, corr = extract(loc+'/'+f, T)
                #corr = extract(loc+'/'+f)
                #loc = './loose2/'+dir+'.'+dir2+'.'+f
                conf_tag = f.split('_')[-1]  # e.g. a001155
                loc = loc_root+'/'+corr_key+'_'+conf_tag
                np.savetxt(loc, corr)

def _write_all2(base, loc_root, T):
    "Write all (loose) correlators corresponding to base."
    #for dir in get_dirs(base):
    #    for dir2 in get_dirs(base+'/'+dir):
    #        for f in get_dirs(base+'/'+dir+'/'+dir2):
    for f in traverse(base):
        #loc = base+'/'+dir+'/'+dir2
        #corr_key, corr = extract(loc+'/'+f)
        corr_key, corr = extract(f, T)
        conf_tag = f.split('_')[-1]  # e.g. a001155
        loc = loc_root+'/'+corr_key+'_'+conf_tag
        np.savetxt(loc, corr)


def write_all(bases, loc_root, T, _concurrent=False):
    if _concurrent:
        pool = Pool(_concurrent)
        pool.starmap(_write_all, product(bases, [loc_root,], [T,]))
        pool.close()
    else:
        for base in bases:
            print('Extracting data from '+base)
            _write_all(base, loc_root, T)

def main(argv):
    #base = "Job100031_a001120/data/loose"
    bases = [d+'/data/loose' for d in get_dirs('.') if "Job" in d]

    write_all(bases, _concurrent=True)
    
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
