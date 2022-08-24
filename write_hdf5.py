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
    res = glob(loc+'/'+_glob, recursive=False)
    return res

def get_keys(loc):
    """Get the correlator keys in loc/.
       
    Obtains a configuration tag, then collects all the keys with that tag.
    """
    #c = collect(loc, "P5-P5*_p000_*")  # Assume this is present. 
    c = collect(loc, "P5-P5*_p000*")  # Assume this is present. 
    # ^ Fix bug if not ^
    #nconfs = len(c)
    conf_tag = c[0].split('_')[-1]  # Get a configuration tag.
    get_key = lambda path: '_'.join(os.path.basename(path).split('_')[:-1])
    keys = [get_key(c) for c in collect(loc, "*_"+conf_tag)]
    #nkeys = len(keys)
    return keys

def get_keys2(loc):
    """Get the correlator keys in loc/.
       
    Obtains a configuration tag, then collects all the keys with that tag.
    """
    c = collect(loc, "P5-P5*")  # Assume this is present. 
    # ^ Fix bug if not ^
    #nconfs = len(c)
    conf_tag = c[0].split('_')[-1]  # Get a configuration tag.
    get_key = lambda path: '_'.join(os.path.basename(path).split('_')[:-1])
    keys = [get_key(c) for c in collect(loc, "*_"+conf_tag)]
    #nkeys = len(keys)
    return keys

def get_keys_tsrcs(loc):
    c = collect(loc, "P5-P5*_p000*")  # Assume this is present. 
    # ^ Fix bug if not ^
    #nconfs = len(c)
    conf_tag = c[0].split('_')[-1]  # Get a configuration tag.
    src_tag = c[0].split('_')[-2]  # Get a source tag.
    get_key = lambda path: '_'.join(os.path.basename(path).split('_')[:-2])
    get_tsrc = lambda path: path.split('_')[-2]
    keys = [get_key(c) for c in collect(loc, "*_"+src_tag+'_'+conf_tag)]
    tsrcs = [get_tsrc(c) for c in collect(loc, keys[0]+'*_'+conf_tag)]
    #nkeys = len(keys)
    return keys, tsrcs


#may want to structure "./loose" better to avoid large 'ls's.
def write_data(loc, f, T):
    print("Building hdf5:")
    for key in get_keys(loc):
        print(key)
        corrs = sorted(collect(loc, key+"*"))
        #print(corrs)
        dat = np.array([np.loadtxt(corr) for corr in corrs])
        f.create_dataset(name='data/'+key, data=dat, maxshape=(None, T),
                         compression='gzip', shuffle=True)
        for path in glob(loc+'/'+key+"*", recursive=False):
            os.remove(path)
   
    '''
    # Here this is picking up correlators unique to the 'b' stream.
    # You should improve get_keys() so it picks up all unique keys.
    for key in get_keys2(loc):
        print(key)
        corrs = collect(loc, key+"*")
        dat = np.array([np.loadtxt(corr) for corr in corrs])
        f.create_dataset(name='data/'+key, data=dat, 
                         compression='gzip', shuffle=True)
        for path in glob(loc+'/'+key+"*", recursive=False):
            #print(path)
            os.remove(path)
    '''

def main():
    f = h5py.File("mytestfile2.hdf5", "w")

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
            print(key)
            print(len(dat[key]))
            assert(len(dat[key]) == nconf)
        print("Every key has {0} confs".format(nconf))

        #print(f["pi.m0.002426-m0.002426-p000.corr2pt"]) #_a001085"])

if __name__ == "__main__":
    with timing():
        #main()
        #test()
        test2()
