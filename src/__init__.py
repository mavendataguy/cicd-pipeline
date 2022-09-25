"""
This is about the addition and substratcion functions

"""
__version__='0.1.0'

import os

def include(filename):
    if os.path.exists(filename): 
        execfile(filename)
        
include('./utils/common.py')
