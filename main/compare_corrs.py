#!/usr/bin/env python3
# Andrew Lytle
# Oct 2023

import sys
import h5py
import numpy as np

def get_hdf5(h5fname):
    return h5py.File(h5fname)

def get_dat(h5fname):
    f = h5py.File(h5fname)
    return f['data']

def compare_corrs(d1, d2):
    for c in d2.keys():
        for i in range(len(d2[c])):
            if not np.array_equal(d1[c][i],d2[c][i]):
                print(i)
                print(c)
                print(d1[c][i] - d2[c][i])
    print('done')

def main(argv):
    if len(argv) != 2:
        print("Two hdf5 filenames expected as args.")
        sys.exit(1)
    f1, f2 = argv
    d1 = get_dat(f1)
    d2 = get_dat(f2)
    compare_corrs(d1, d2)

if __name__ == "__main__":
    main(sys.argv[1:])
