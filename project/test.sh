#!/bin/bash

# Install Python packages
pip install --upgrade pip
pip install io
pip install requests 
pip install zipfile
pip install pandas
pip install pysqlite3

python3 projectpipeline.py
python3 test.py
