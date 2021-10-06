#!/usr/bin/env python3
# Andrew Lytle
# Aug 2021

import h5py
from itertools import product
from multiprocessing import Pool

from stage import get_tars, transfer, cleanup
from extract_milc_corrs import get_dirs, write_all
from write_hdf5 import write_data

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
