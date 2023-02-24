#!/usr/bin/env python3
# Andrew Lytle
# Feb 2023

"""Read input yaml and prepare environment for cache build.
"""

import os
import sys
import yaml

def read_yaml(fname):
    with open(fname, "r") as f:
        params =  yaml.safe_load(f)
    return params

def bind_params(params):
    """Bind yaml parameters to local variables.
    """

    src_root = params['src_root']  # Where Job*.tar.bzs are stored.
    #stage_root = '/tmp/stage'  # Where to untar Job*.tar.bz2.
    stage_root = params['stage_root']  # Where to untar Job*.tar.bz2.
    extract_root = params['extract_root']  # Intermediate extract location.
    ave_root = params['ave_root']  # Average over time sources location.
    h5name = params['h5name']  # Output hdf5 location/name.
    log = params['log']  # Output log location/name.
    T = params['T']  # Time extent of lattice.
    _concurrent = params['concurrent']  # No. of concurrent processes.
    nsrc = params['nsrc']  # Time sources/config.
    stream = params['stream']  # Stream label.
    start = params['start']  # Start config.
    end = params['end']  # End config (included).
    delta = params['delta']  # Config spacing.

    return (src_root, stage_root, extract_root, ave_root, h5name, 
            log, T, _concurrent, nsrc, stream, start, end, delta)

def init_dirs(dirs):
    """Ensure build dirs exist and are empty.
    """
    for _dir in dirs:
        if not os.path.isdir(_dir):
            os.mkdir(_dir)
        if len(os.listdir(_dir)) != 0:
            raise Exception(f"Directory {_dir} not empty.")

def test():
    init_dirs(['./test1', './test2'])

if __name__ == "__main__":
    test()