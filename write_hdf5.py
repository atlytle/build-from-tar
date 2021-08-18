#!/usr/bin/env python3
# Andrew Lytle
# Dec 2020

import os
from glob import glob
import subprocess
import sys
sys.path.append(os.environ['HOME'] + '/allhisq')

import h5py
import numpy as np

from timing import timing

def collect(loc, _glob):
    "Filenames of form loc/glob."
    res = subprocess.run(["ls"] + glob(loc+'/'+_glob), capture_output=True)
    res = res.stdout.decode().split()
    return res

def get_keys(loc):
    #c = collect(loc, "pi.*-p000.*")  # Assume this is present.
    c = collect(loc, "P5-P5*_p000_*")  # Assume this is present. 
    # ^ Fix bug if not ^
    #nconfs = len(c)
    conf_tag = c[0].split('_')[-1]  # Get a configuration tag.
    get_key = lambda path: '_'.join(os.path.basename(path).split('_')[:-1])
    keys = [get_key(c) for c in collect(loc, "*_"+conf_tag)]
    #nkeys = len(keys)
    return keys

#may want to structure "./loose" better to avoid large 'ls's.
def write_data(loc, f):
    print("Building hdf5:")
    for key in get_keys(loc):
        print(key)
        corrs = collect(loc, key+"*")
        dat = np.array([np.loadtxt(corr) for corr in corrs])
        #f.create_dataset(key, data=dat) #compression='gzip', shuffle=True?
        f.create_dataset(name='data/'+key, data=dat, 
                         compression='gzip', shuffle=True)
        
def main():
    f = h5py.File("mytestfile2.hdf5", "w")
    #f.create_dataset("data", (100,), dtype='i')

    write_data("./loose2", f)
    
    f.close()

def test():
    with h5py.File('mytestfile2.hdf5', 'r') as f:
        print(f.keys())
        dat = f['data']
        k = list(dat.keys())[0]
        print(k)
        print(dat[k][0])
        print(len(list(dat.keys())))
        
        # Check nconf is the same for every correlator.
        nconf = None
        for key in dat.keys():
            if not nconf:
                nconf = len(dat[key])
            print(len(dat[key]))
            assert(len(dat[key]) == nconf)
        print("Every key has {0} confs".format(nconf))

        #print(f["pi.m0.002426-m0.002426-p000.corr2pt"]) #_a001085"])

if __name__ == "__main__":
    with timing():
        #main()
        test()
