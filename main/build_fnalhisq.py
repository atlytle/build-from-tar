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
from stage import get_tars, filter_tars
from timing import timing, Write_print
from write_hdf5 import write_data
from write_hdf5 import get_keys, get_keys_tsrcs

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
