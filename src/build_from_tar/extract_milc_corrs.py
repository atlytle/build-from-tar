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

from build_from_tar.timing import timing

REGEX = re.compile('correlator_key:')  # Pattern signaling end of header.

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

def imag_part(lines):
    "Imaginary part of correlation functions."
    return [float(line.split()[2]) for line in lines]

def process(corr_key):
    "Grab real or imaginary part of correlator based on key."
    # Correlators where signal is in imaginary part.
    icorrs = ['A4-A4_T14-V4',
              'A4-A4_T24-V4',
              'A4-A4_T34-V4',
              'P5-P5_V1-S_T',                                    
              'P5-P5_V2-S_T',                      
              'P5-P5_V3-S_T']
    for c in icorrs:
        if corr_key.startswith(c):
            return imag_part
    return real_part

def extract(fname, T):
    with open(fname, 'r') as f:
        lines = f.readlines()
    #_corr_key = corr_key(lines)  # Assumes only 1 unique per file.
    #lines = remove_headers(lines, T)
    result = []
    for i, line in enumerate(lines):
        if REGEX.match(line):
            _corr_key = line.strip().split()[-1]
            # Skip "..." line after the match.
            result.append(lines[i+2:i+2+T])
            break
    ri_part = process(corr_key)  # Which part of corr to get.
    nums = np.array(list(map(ri_part, result)))
    return _corr_key, np.average(nums, axis=0)  # I think this averages over time sources..

def corr_key(lines):
    """Extract the correlator key from milc output file. 
    
    e.g. correlator_key:     P5-P5...
    """
    for line in lines:
        if REGEX.match(line):
            return line.strip().split()[-1]

def extract_all3(fname, T):
    """Extract all correlator keys and correlator data from milc output file.
    
    extract(fname, T) only obtains the first correlator in the output,
    this version gets all of them. 
    NOTE here there is just one time source/output file.
    """
    corr = {}  # Store correlators in a dictionary.
    with open(fname, 'r') as f:
        lines = f.readlines()
    
    for i, line in enumerate(lines[:]):
        if REGEX.match(line):
            corr_key = line.strip().split()[-1]
            # Skip "..." line after the match.
            data = lines[i+2:i+2+T]
            ri_part = process(corr_key)  # Which part of corr to get.
            nums = np.array(list(map(ri_part, [data,])))
            # nb: np.average([[1,2,3],[5,6,7]], axis=0) = np.array([3.,4.,5.]) 
            # (changes shape, so ave([[1,2,3],], axis=0) = [1,2,3])
            corr[corr_key] = np.average(nums, axis=0)
    return corr
    
def extract_all_witht(fname, T):
    """Extract all correlator keys and correlator data from milc output file.
    
    extract(fname, T) only obtains the first correlator in the output,
    this version gets all of them. 
    NOTE here there are multiple tsrc values in a single file, and this
    averages over them.
    """
    corr = {}  # Store correlators in a dictionary.
    with open(fname, 'r') as f:
        lines = f.readlines()

    for i, line in enumerate(lines[:]):
        if REGEX.match(line):
            corr_key = line.strip().split()[-1]
            if corr_key not in corr:
                corr[corr_key] = []
            # Skip "..." line after the match.
            data = lines[i+2:i+2+T]
            ri_part = process(corr_key)  # Which part of corr to get.
            nums = np.array(list(map(ri_part, [data,])))[0]
            print(f"{nums = }")
            corr[corr_key].append(nums)

    for corr_key in corr:
        corr[corr_key] = np.average(corr[corr_key], axis=0)

    return corr

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
                src_tag = base.split('_')[-2]  # e.g. F027
                loc = loc_root+'/'+corr_key+'_'+src_tag+'_'+conf_tag
                #if corr_key.startswith('P5-P5_S-S'):
                np.savetxt(loc, corr)

def _write_all3(base, loc_root, T):
    "Write all (loose) correlators corresponding to base. Uses extract_all()."
    for dir in get_dirs(base):
        for dir2 in get_dirs(base+'/'+dir):
            for f in get_dirs(base+'/'+dir+'/'+dir2):
                loc = base+'/'+dir+'/'+dir2
                conf_tag = f.split('_')[-1]  # e.g. a001155
                src_tag = base.split('_')[-2]  # e.g. F027
                corrs = extract_all3(loc+'/'+f, T)
                for corr_key in corrs:
                    loc = loc_root+'/'+corr_key+'_'+src_tag+'_'+conf_tag
                    #if corr_key.startswith('P5-P5_S-S') or \
                    #   corr_key.startswith('P5-P5_RW_RW'):
                    np.savetxt(loc, corrs[corr_key])

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


def write_all3(bases, loc_root, T, _concurrent=False):
    if _concurrent:
        pool = Pool(_concurrent)
        pool.starmap(_write_all3, product(bases, [loc_root,], [T,]))
        pool.close()
    else:
        for base in bases:
            print('Extracting data from '+base)
            _write_all3(base, loc_root, T)

def _write_all_witht(base, loc_root, T):
    "Write all correlators corresponding to base. Uses extract_all_witht()."
    for dir in get_dirs(base):
        for dir2 in get_dirs(base+'/'+dir):
            for f in get_dirs(base+'/'+dir+'/'+dir2):
                loc = base+'/'+dir+'/'+dir2
                conf_tag = f.split('_')[-1]  # e.g. a001155
                #src_tag = base.split('_')[-2]  # e.g. F027
                corrs = extract_all_witht(loc+'/'+f, T)
                print(f"{loc = }")
                print(f"{corrs = }")
                for corr_key in corrs:
                    loc = loc_root+'/'+corr_key+'_'+conf_tag
                    np.savetxt(loc, corrs[corr_key])

def write_all_witht(bases, loc_root, T, _concurrent=False):
    if _concurrent:
        pool = Pool(_concurrent)
        pool.starmap(_write_all_witht, product(bases, [loc_root,], [T,]))
        pool.close()
    else:
        for base in bases:
            print('Extracting data from '+base)
            _write_all_witht(base, loc_root, T)

def main(argv):
    #base = "Job100031_a001120/data/loose"
    bases = [d+'/data/loose' for d in get_dirs('.') if "Job" in d]

    write_all(bases, _concurrent=True)
    
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
