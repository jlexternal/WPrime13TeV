# !/bin/python
import subprocess
import glob
import copy
import re
import sys
import os
from optparse import OptionParser


parser = OptionParser()

parser.add_option('--file', metavar='F', type='string', action='store',
                  dest='file',
                  default='analysis_wprimeR_allhad_limits.py',
                  help='analysis file')


(options, args) = parser.parse_args()

argv = []

outfile = options.file.split('.')[0] 
retval = os.getcwd() + '/'
os.chdir(retval + outfile + '/res')
commands = [
     
    'cp ../../untarthem.py .',
    'python untarthem.py',
    'cp ../share/default.tgz .',
    'tar -zxvf default.tgz',
    'tar -zxvf analysis.tgz',
    'mkdir analysis/cache/',
    'cp analysis/*.cfg analysis/cache',
    'mv *.db analysis/cache',
    'sed "s/RSTEP/1/g " ../../' + options.file + '  > ./analysis.py',
    '../../utils2/theta-auto.py',
    'mkdir results',
    'mv *limit.txt results/',
    'cp analysis.py results/',
    'cp *.root results/',
    'tar -cz results/ > results_' + outfile + '.tgz'
    ]

for s in commands :
    print 'executing ' + s
    subprocess.call( [s], shell=True )
#os.chdir(retval )
