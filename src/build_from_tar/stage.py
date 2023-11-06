#!/usr/bin/env python3
# Andrew Lytle
# Dec 2020

"""Copy, untar, and remove directories for extracting correlator info.
"""

import concurrent.futures
import os
import subprocess
import sys
import time

from build_from_tar.timing import timing


def get_tars(loc, by_cfg=False):
    res = subprocess.run(["ls", loc], capture_output=True)
    res = res.stdout.decode().split()
    tars = [r for r in res if ("Job" in r) and (".tar.bz2" in r)]
    
    def get_cnfg(tar):
        "Job##_F##_a##.tar.bz2 -> a##"
        return tar.split('_')[-1].split('.')[0]
    if by_cfg:
        cnfgs = list(set(map(get_cnfg, tars)))
        print('cnfgs:', cnfgs)
        res = {cfg:[] for cfg in cnfgs}
        for cfg in cnfgs:
            res[cfg] = [tar for tar in tars if cfg in tar]
            #res.append([tar for tar in tars if cfg in tar])
        return res
    else:
        return tars
    #check = [r for r in res if ("Job" in r) and (".tar.bz2" in r)]
    #try:
    #    assert len(res) == len(check)
    #except AssertionError:
    #    raise Exception("File(s) in {0} not of form Job*.tar.bz2".format(loc))
    #return tars

def transfer(src_root, bases, dest_root, _concurrent=False):
    "Untar src_root/base.tar.bz2 into dest_root/base for base in bases."
    def _transfer(src_root, base, dest_root):
        file = base+".tar.bz2"  # base=Job93776_a000880 e.g.

        src = src_root+'/'+file
        dest = dest_root+'/'+base
        tar = dest+'/'+file

        # Copy and extract into temp directory 
        #(or extract directly, may be faster).
        print("Staging " + base)
        subprocess.run(["mkdir", dest])
        subprocess.run(['tar', 'xvjf', src, '-C', dest],
                        stdout=subprocess.DEVNULL)
    
    if _concurrent:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for base in bases:
                executor.submit(_transfer, src_root=src_root, 
                                base=base, dest_root=dest_root)
                #time.sleep(1)
    else:
        for base in bases:
            _transfer(src_root, base, dest_root)

def cleanup(bases, dest_root, _concurrent=False):
    "Remove dest_root/base for base in bases."
    def _cleanup(base, dest_root):
        print("Removing " + base)
        subprocess.run(["rm", "-rf", dest_root+'/'+base])
    
    if _concurrent:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for base in bases:
                executor.submit(_cleanup, base=base, dest_root=dest_root)
    else:
        for base in bases:
            _cleanup(base, dest_root)

def filter_tars(tars, cfgs, nsrc):
    "Collect tars in cfgs with nsrc sources (i.e. completed configs)."
    # Gather up only completed configurations in cfgs.
    for cfg in list(tars.keys()):
        if len(tars[cfg]) != nsrc:
            tars.pop(cfg)  # Throw these out.
            continue
        if cfg not in cfgs:
            tars.pop(cfg)
    return tars

def main():
    #root = "/project/fermilab/heavylight/hisq/allHISQ"
    #root += "/a0.15/l3248f211b580m002426m06730m8447/run1/tar"
    src_root = './tar'
    #tars = get_tars(src_root)[0:40]
    tars = get_tars(src_root)[0:10]
    #print(tars)
    bases = [tar.rstrip('.tar.bz2') for tar in tars]
    stage_root = './stage'
    #_concurrent = True
    extract_root = './loose'
    
    # Stage tarballs for processing.
    with timing():
        transfer(src_root, bases, stage_root, _concurrent=True)
    
    # Extract correlator info, deposit in loose2/.
    # Note here you simply sum over source times, but write_all
    # in extract_milc_corrs.py should really do tsm.
    fnames = [d+'/data/loose' for d in get_dirs(stage_root) if "Job" in d]
    with timing():
        write_all(fnames, extract_root, _concurrent=True)
    
    # Remove untarred stuff.
    with timing():
        cleanup(bases, stage_root, _concurrent=True)
        #subprocess.run(["rm", "./loose2/*"]) # need glob to do this
    
if __name__ == '__main__':
    with timing():
        main()
