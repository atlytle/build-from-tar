#!/usr/bin/env python3
# Andrew Lytle
# Nov 2021

import sys
import h5py

#h5fname = 'l64192f211b700m00316m0158m188-run2_804-876_12cfgs.hdf5'

def rename(h5fname, modify=True):
    # Set io mode.
    if modify:
        mode = 'r+'
    else:
        mode = 'r'

    with h5py.File(h5fname, mode) as f:
        dat = f['data']
        keys = list(dat.keys())
        n = 0
        for k in keys:
            n += 1
            print(k)
            print(dat[k].shape)
            if modify:
                knew = k[:-5]  # Remove '-fine' from tags.
                print(knew)
                dat[knew] = dat[k]
                del dat[k]
        print(n)
        print(len(keys))

if __name__ == "__main__":
    h5fname = sys.argv[1]
    rename(h5fname, modify=True)
