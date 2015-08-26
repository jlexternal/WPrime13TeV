# !/bin/python
import subprocess
import glob
import copy
import re
import sys

from optparse import OptionParser


parser = OptionParser()

parser.add_option('--file', metavar='F', type='string', action='store',
                  dest='file',
                  default='analysis_wprimeR_allhad_limits.py',
                  help='analysis file')


parser.add_option('--crabfile', metavar='F', type='string', action='store',
                  dest='crabfile',
                  default='crabThetaGrid.cfg',
                  help='crab file')


(options, args) = parser.parse_args()

argv = []

outfile = options.file.split('.')[0] 


uidir = outfile



commands = [
    'rm analysis.py',
    'rm -rf analysis/',
    'rm analysis.tgz',
    'sed "s/RSTEP/0/g " ' + options.file + '  > ./analysis.py',
    './utils2/theta-auto.py',
    'tar -cz analysis/ > analysis.tgz',
    'cp analysis.tgz ' + outfile + '.tgz',
    'crab -create  crab -USER.ui_working_dir ' + uidir + ' -create -cfg ' + options.crabfile,
    'crab -submit -c ' + uidir,
    'crab -status -c ' + uidir
    ]

for s in commands :
    print 'executing ' + s
    subprocess.call( [s], shell=True )

