#!/usr/bin/env python3
# Andrew Lytle
# Aug 2021

import h5py
import sys
from itertools import product
from multiprocessing import Pool

#from extract_milc_corrs import get_dirs, write_all

sys.path.append('/lustre1/heavylight/atlytle/build-from-src')
from extract_milc_corrs import get_dirs
from extract_milc_corrs import write_all3 as write_all
from stage import get_tars, transfer, cleanup
from write_hdf5 import write_data, get_keys_tsrcs

from timing import timing

def _build_by_base(src_root, stage_root, extract_root, T, base):
    transfer(src_root, [base,], stage_root, _concurrent=False)
    #dname = stage_root+'/'+base+'/data/loose'
    dname = stage_root+'/'+base+'/data'
    write_all([dname,], extract_root, T, _concurrent=False)
    cleanup([base,], stage_root, _concurrent=False)

def build_by_base(src_root, stage_root, extract_root, T, 
                  bases, _concurrent=False):
    if _concurrent:
        pool = Pool(_concurrent)  # _concurrent = number of processes.
        args = product([src_root,], [stage_root,], [extract_root,], [T,], bases)
        pool.starmap(_build_by_base, args)
        pool.close()
    else:
        for base in bases:
            _build_by_base(src_root, stage_root, extract_root, T, base)

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

def main():
    src_root = './tar'  # Where Job*.tar.bz2 are stored.
    tars = get_tars(src_root)[:8]  # Collect up tars.
    bases = [tar.rstrip('.tar.bz2') for tar in tars]
    stage_root = './stage'  # Where to untar Job*.tar.bz2.
    extract_root = './loose'
    h5name = 'mytestfile2.hdf5'
    _concurrent=True

    with timing():
        build_by_base(src_root, stage_root, extract_root, bases, _concurrent)

    # Enter processed data in hdf5 cache.
    print('Writing correlators to hdf5')
    f = h5py.File(h5name, "w")
    with timing():
        write_data(extract_root, f)
    f.close()

if __name__ == '__main__':
    with timing():
        main()
