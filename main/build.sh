#!/bin/bash

script="/lustre1/heavylight/atlytle/build-from-src/main/build_allhisq.py"
input="build.yaml"

nohup nice -n 6 python -u $script $input 2>&1 & #>> $log 2>&1 &
