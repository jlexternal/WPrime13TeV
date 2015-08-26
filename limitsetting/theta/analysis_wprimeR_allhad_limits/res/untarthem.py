# !/bin/python
import subprocess
import glob
import copy
import re
import sys




# Get the analysis files
files = glob.glob( '*.tgz' )

for file in files :
    print '-------------------------------------------------'
    s = 'tar -zxvf ' + file
    print 'executing ' + s
    subprocess.call( [s], shell=True )

