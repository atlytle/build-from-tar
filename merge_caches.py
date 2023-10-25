#!/usr/bin/env python3
# Andrew Lytle
# Jan 2022

import os
import sys

import h5py
import numpy as np

def append_data(h5fname):
    with h5py.File(h5fname, "a") as f:
        dat = f['data']
        k = list(dat.keys())[0]
        print(dat[k].shape)
        print(dat[k])
        copy = dat[k][0,:]
        print('copy')
        print(copy)
        dat[k].resize((2, 192))
        print(dat[k].shape)
        dat[k][1] = copy
        print(dat[k][:])

#append_data('mytestfile2.hdf5')

def merge_hdf5(fname1, fname2, fmerge):
    with h5py.File(fname2, "r") as f2:
        d2 = f2['data']
        keys2 = list(d2.keys())
        with h5py.File(fname1, "r") as f1:
            d1 = f1['data']
            keys1 = list(d1.keys())

            print(len(keys2))
            print(len(keys1))
            print(len(set(keys1).intersection(set(keys2))))
    
            with h5py.File(fmerge, 'w') as f:
                # We want to ensure keys are the same for both hdf5 files...
                for key in keys1:
                    print(d1[key])
                    print(d2[key])
                    test = np.append(d1[key][:], d2[key][:], axis=0)
                    print('test.shape')
                    print(test.shape)
                    f.create_dataset(name='data/'+key, 
                             data = np.append(d1[key][:], d2[key][:], axis=0),
                             shape = test.shape,
                             compression='gzip', 
                             shuffle=True)
            #f['data/'+key][:,:] = d1[key][:].append(d2[key][:])
    
    #with h5py.File('merge.hdf5', 'r') as f:
    #    print(f['data'][keys1[0]][:])

def merge_multi(to_merge, outname="merged_complete.hdf5"):
    """Merge multiple hdf5 caches.
        
    Args:
        to_merge - List of hdf5 filenames to merge together.
        outname - Name of final merged hdf5 file.
    """
    out_temp = 'merging.hdf5'
    if outname == out_temp or outname == "merged.hdf5":
        print(outname, "clobbers a reserved name, please rename.")
        sys.exit(1)

    if len(to_merge) == 2:
        # Reproduce merge_hdf5 behavior.
        f1, f2 = to_merge
        merge_hdf5(f1, f2, outname)
    elif len(to_merge) > 2:
        f1, f2, *rest = to_merge
        merge_hdf5(f1, f2, out_temp)
        os.rename(out_temp, "merged.hdf5")
        rest.insert(0, "merged.hdf5")
        merge_multi(rest, outname)
    elif len(to_merge) < 2:
        print("to_merge arg list must have length >= 2.")
        print("to_merge:")
        print(to_merge)
        sys.exit(1)
    
    # Tidy up.
    if os.path.exists("merged.hdf5"):
        os.remove("merged.hdf5")

if __name__ == "__main__":
    #f1 = 'l64192f211b700m00316m0158m188-run2_804-1740_157cfgs-P5-P5.hdf5'
    #f2 = 'l64192f211b700m00316m0158m188-run2_1746-2088_58cfgs-P5-P5.hdf5'
    #fmerge = 'l64192f211b700m00316m0158m188-run2_804-2088_215cfgs-P5-P5.hdf5'
    #merge_hdf5(f1, f2, fmerge)
    merge_multi(sys.argv[1:])

