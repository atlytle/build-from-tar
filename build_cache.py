#!/usr/bin/env python3
# Andrew Lytle
# Aug 2021

import h5py

from stage import get_tars, transfer, cleanup
from extract_milc_corrs import get_dirs, write_all
from write_hdf5 import write_data

from timing import timing

def main():
    src_root = './tar'  # Where Job*.tar.bz2 are stored.
    tars = get_tars(src_root)[:4]  # Collect up tars.
    bases = [tar.rstrip('.tar.bz2') for tar in tars]
    stage_root = './stage'  # Where to untar Job*.tar.bz2.
    extract_root = './loose'  # Where to extract corr data to.
    h5name = 'mytestfile2.hdf5'  # Cache name.

    # Stage tarballs for processing.
    print('Staging tars')
    with timing():
        transfer(src_root, bases, stage_root, _concurrent=True)
    
    # Traverse milc output files and write info to intermediate files.
    print('Extracting correlator info')
    with timing():
        # dnames is just the directories that need processing.
        # e.g. dname = "Job100031_a001120/data/loose"
        dnames = [stage_root+'/'+d+'/data/loose' 
                  for d in get_dirs(stage_root) if "Job" in d]
        write_all(dnames, extract_root, _concurrent=True)
    
    # Remove untarred stuff.
    print('Removing untarred jobs')
    with timing():
        cleanup(bases, stage_root, _concurrent=True)

    # Enter processed data in hdf5 cache.
    print('Writing correlators to hdf5')
    f = h5py.File(h5name, "w")
    write_data(extract_root, f)
    f.close()
    
if __name__ == '__main__':
    main()
