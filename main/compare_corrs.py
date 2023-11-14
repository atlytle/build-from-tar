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

d1 = get_dat('l4864f211b600m001907m05252m6382-HISQscript.hdf5')
#d1 = get_dat('test.hdf5_')
d2 = get_dat('test.hdf5')
#c = 'P5_P5_RW_RW_d_d_k0.08574_m0.001907_p000'

#print(d1[c][0])
#print(d2[c][0])

for c in d2.keys():
    #print(c)
    for i in range(len(d2[c])):
        #print(i)
        ##print(d1[c][i] - d2[c][i])
        if not np.array_equal(d1[c][i],d2[c][i]):
            print(c)
print('done')
