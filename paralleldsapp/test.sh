#! /bin/bash
export NUMBER_OF_WORKERS=2
# export WORKER_NUMBER=$1
# ./busywork
python3 paralleldsapp.py in out
