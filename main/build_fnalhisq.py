#!/usr/bin/env python3
# Andrew Lytle
# Oct 2021

import os
import sys
import h5py
import re
import yaml
import numpy as np
from itertools import product
from multiprocessing import Pool

sys.path.append('/lustre1/heavylight/atlytle/build-from-src')
print(sys.path)
from build_by_base import build_by_base_witht
from init import read_yaml, bind_params, init_dirs
from rename_dat import rename
from stage import get_tars
from timing import timing, Write_print
from write_hdf5 import write_data
from write_hdf5 import get_keys, get_keys_tsrcs

def _consolidate_tsrc(extract_root, ave_root, cfg, key, tsrcs):
    print(key)
    cname = lambda key, tsrc: extract_root+'/'+key+'_'+tsrc+'_'+cfg
    dat = np.array([np.loadtxt(cname(key,t)) for t in tsrcs])
    dat = np.average(dat, axis=0)
    for t in tsrcs:
        os.remove(cname(key, t))
    np.savetxt(ave_root+'/'+key+'_'+cfg, dat)


def consolidate_tsrc(extract_root, ave_root, cfg, _concurrent=False):
    keys, tsrcs = get_keys_tsrcs(extract_root)
    print(f"{len(keys) = }")
    #cname = lambda key, tsrc: extract_root+'/'+key+'_'+tsrc+'_'+cfg
    if _concurrent:
        pool = Pool(_concurrent)  # _concurrent = number of processes.
        args = product([extract_root,], [ave_root,], [cfg,], keys, [tsrcs,])
        pool.starmap(_consolidate_tsrc, args)
        pool.close()
    else:
        i=0
        for key in keys:
            i+=1
            print(i)
            _consolidate_tsrc(extract_root, ave_root, cfg, key, tsrcs)
        #dat = np.array([np.loadtxt(cname(key,t)) for t in tsrcs])
        #dat = np.average(dat, axis=0)
        #for t in tsrcs:
        #    os.remove(cname(key, t))
        #np.savetxt(ave_root+'/'+key+'_'+cfg, dat)

def filter_tars(tars, cfgs, nsrc):
    "Collect tars in cfgs with nsrc sources (i.e. completed configs)."
    # Gather up only completed configurations in cfgs.
    for cfg in list(tars.keys()):
        #if len(tars[cfg]) != nsrc:
        #    tars.pop(cfg)  # Throw these out.
        #    continue
        if cfg not in cfgs:
            tars.pop(cfg)
    return tars
    
def main(argv):
    params = read_yaml(argv[0])  # Read input yaml.
    
    (src_root, stage_root, extract_root, ave_root, h5name, 
    log, T, _concurrent, nsrc, stream, start, end, delta) = bind_params(params)

    # Check build directories exist.
    init_dirs([stage_root, extract_root, ave_root])
    
    # Generate dictionary of tars to unpack.
    cfgs = [f"{stream}{x:06d}" for x in range(start, end+1, delta)]
    print(len(cfgs))
    tars = get_tars(src_root, by_cfg=True)  # Collect up tars.
    print(tars.keys())
    tars = filter_tars(tars, cfgs, nsrc)
    print(tars.keys())
    print(len(tars))

    
    for cfg in list(tars.keys())[:]:
        bases = [re.sub(r"\.tar.bz2$", "", tar) for tar in tars[cfg]][:]
        with timing():
            build_by_base_witht(src_root, stage_root, extract_root, 
                          T, bases, _concurrent)
        
        # Serial step here to average over tsrc.
        # With many configs could parallel over cfgs in main loop
        # could launch another parallel bit here, over correlator keys..
        
        ###print("Consolidating timesources for cfg", cfg)
        ###with timing():
        ###    consolidate_tsrc(extract_root, ave_root, cfg, _concurrent)
    
    # Enter processed data in hdf5 cache.
    print('Writing correlators to hdf5')
    f = h5py.File(h5name, "w")
    with timing():
        write_data(ave_root, f, T, _glob="P5_P5*_p000*")
    f.close()

    # Remove "-fine" from correlator names. How is this handled elsewhere?
    ###rename(h5name, modify=True)

if __name__ == '__main__':
    # Kludge to direct output to log.
    params = read_yaml(sys.argv[1])  # Read input yaml.
    with Write_print(params['log']):
        with timing():
            main(sys.argv[1:])
