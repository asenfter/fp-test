#!/bin/bash

# create a virtual environment if it does not exists
if [ ! -d "./venv" ]; then
    python3 -m virtualenv --python=python3.6 --clear venv
    # activate the environment and install all required packages
    source ./venv/bin/activate
    pip install -r requirements.txt
    deactivate
fi

# run the unittest
source ./venv/bin/activate
python -m unittest -v test_fastparquet.py