#!/usr/bin/env python3
# Andrew Lytle
# Aug 2021

import os
import re

import numpy as np

from extract_milc_corrs import get_dirs, extract, corr_key, real_part

REGEX = re.compile('correlator_key:')  # Pattern signaling end of header.
REGEX_TSRC = re.compile('quark_source_origin:')
T = 48

def to_nums(lines):
    return np.array(list(map(real_part, lines)))

def get_tsm_dirs(stage_root, base):
    """Traverse directories and get fine, loose correlators.
    """
    lname = stage_root+'/'+base+'/data/loose'
    fname = stage_root+'/'+base+'/data/fine'

    # find matching file name..
    for dir in get_dirs(lname):
        for dir2 in get_dirs(lname+'/'+dir):
                for f in get_dirs(lname+'/'+dir+'/'+dir2):
                    lloc = lname+'/'+dir+'/'+dir2+'/'+f
                    floc = fname+'/'+dir+'/'+dir2+'/'+f
                    #assert(os.path.isfile(floc))  # Does matching solve exist?
                    yield lloc, floc

class CorrTSM:
    def __init__(self, lname, fname):
        pass 

def parse_tsrc(line):
    res = int(line.split()[-2])
    return res
# note getting tsrc needs only happen once per Job.. not every file necc.

def extract_with_tsrc(fname):
    with open(fname, 'r') as f:
        lines = f.readlines()
    _corr_key = corr_key(lines)
    result = []
    for i, line in enumerate(lines):
        if REGEX_TSRC.match(line):
            _tsrc = parse_tsrc(line)
        if REGEX.match(line):
            # Skip "..." line after the match.
            #result.append((_tsrc, to_nums(lines[i+2:i+2+T])))
            _corr_key = corr_key([line,])
            result.append((_corr_key,
                           _tsrc, np.array(real_part(lines[i+2:i+2+T]))))
    #print(result)
    return _corr_key, result

def extract_with_tsm(lname, fname):
    lcorr_key, lcorr = extract_with_tsrc(lname)
    fcorr_key, fcorr = extract_with_tsrc(fname)
    #print(lcorr_key)
    #print(fcorr_key)
    #assert(lcorr_key.rstrip('-loose') == fcorr_key.rstrip('-fine'))
    #print(len(fcorr))
    #print(len(lcorr))
    #print(lcorr)
    return lcorr, fcorr

def do_tsm(lcorr, fcorr, tsm=True):
    ckey = fcorr[0][0].rstrip('-fine')+'-loose'  # Assume only 1 ckey
    _tsrc = fcorr[0][1]
    fdat = fcorr[0][2]
    _lcorr = [x for x in lcorr if x[0]==ckey]
    tsm_match = [x[2] for x in _lcorr if (x[1] == _tsrc)]
    lave = np.average(np.array([x[2] for x in _lcorr]), axis=0)
    if len(tsm_match) == 1:
        tsm_match = tsm_match[0]
    else: 
        raise Exception()
    #print(len(_lcorr))
    #print(ckey)
    #print(_tsrc)
    #print(fdat - tsm_match)
    #print(lave + fdat - tsm_match)
    if tsm:
        res = lave + fdat - tsm_match
    else:
        res = lave
    return ckey.rstrip('-loose'), res

def write_w_tsm(stage_root, extract_root, base):
    for lname, fname in get_tsm_dirs(stage_root, base):
        lcorr, fcorr = extract_with_tsm(lname, fname)
        ckey, res = do_tsm(lcorr, fcorr)
        conf_tag = fname.split('_')[-1]  # e.g. a001155
        loc = extract_root+'/'+ckey+'_'+conf_tag
        np.savetxt(loc, res)
 

if __name__ == '__main__':
    stage_root = './stage_tsm'
    extract_root = './loose'
    base='Job548102_a001045.tar.bz2_tar_'

    write_w_tsm(stage_root, extract_root, base)       
