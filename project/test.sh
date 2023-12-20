#!/bin/bash

# Install Python packages
pip install requests 
pip install pandas
pip install pysqlite3


# Print information before running tests
echo "Running tests..."

python3 projectpipeline.py
python3 test.py
