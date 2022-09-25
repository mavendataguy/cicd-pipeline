import os

def include(filename):
    if os.path.exists(filename): 
        execfile(filename)
        
include('./utils/common.py')
