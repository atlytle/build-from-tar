Constructs hdf5 caches of correlator data from raw MILC code data.  
Currently supports building from allhisq and FNAL-HISQ campaign outputs.

## Installation and running
Can be installed from source with `pip install .`  
Main executables: `build_allhisq.py` and `build_fnalhisq.py`,  
`build.yaml` - Example input  
`build.sh` - Example driver script

## yaml input
`src_root` - Location of tar files to process.  
`stage_root` - Temporary staging location of tars being processed.  
`extract_root` - Location where extracted data is written (temporary).  
`ave_root` - Location where average correlator data is written (temporary).  
`h5name` - Name/location of target hdf5 cache.  
`log` - Name/location of output log from build process.  
`T` - Time extent of correlator data [integer].  
`concurrent` - Number of parallel processes used to parse data [int].
             Set `concurrent: 0` to run in serial mode.  
`nsrc` - Number of time sources per configuration [int].
       For FNAL-HISQ data set `nsrc: 1`.  
`stream` - Label of the Monte Carlo stream, e.g. `a004740` -> `stream: a`.  
`start` - Starting configuration number e.g. 4740.  
`end` - Ending configuration number (included).  
`delta` - Spacing between sequential configuration numbers [int].  

## Notes
The FNAL-HISQ data has data for all time sources in a single text file;
the tar files are indexed by a configuration tag only.
Set `ave_root` = `extract_root` and `nsrc: 1` in the input yaml.

The allhisq data is indexed by configuration number and time source.
First the numerical data is extracted into `extract_root`, then it is averaged
over time sources and written to `ave_root`, and finally written to cache.
