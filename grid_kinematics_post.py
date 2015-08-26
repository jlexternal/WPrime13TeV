#! /usr/bin/env python
import re
import os
import subprocess
from os import listdir
from os.path import isfile, join
import glob
import math
import ROOT
from ROOT import *
import sys
from DataFormats.FWLite import Events, Handle
from optparse import OptionParser
parser = OptionParser()
parser.add_option('-c', '--cuts', metavar='F', type='string', action='store',
                  default	=	'default',
                  dest		=	'cuts',
                  help		=	'Cuts type (ie default, rate, etc)')
(options, args) = parser.parse_args()

cuts = options.cuts

import Wprime_Functions	
from Wprime_Functions import *

#Load up cut values based on what selection we want to run 
Cons = LoadConstants()
lumi = Cons['lumi']
ttagsf = Cons['ttagsf']
xsec_wpr = Cons['xsec_wpr']
xsec_wpl = Cons['xsec_wpl']
xsec_wplr = Cons['xsec_wplr']
xsec_ttbar = Cons['xsec_ttbar']
xsec_qcd = Cons['xsec_qcd']
xsec_st = Cons['xsec_st']
nev_wpr = Cons['nev_wpr']
nev_wpl = Cons['nev_wpl']
nev_wplr = Cons['nev_wplr']
nev_ttbar = Cons['nev_ttbar']
nev_qcd = Cons['nev_qcd']
nev_st = Cons['nev_st']


files = sorted(glob.glob("*job*of*.root"))

j = []
for f in files:
	j.append(f.replace('_jo'+re.search('_jo(.+?)_PSET', f).group(1),""))

files_to_sum = list(set(j))

commands = []
commands.append('rm *.log') 
commands.append('rm temprootfiles/*.root')
commands.append('rm -rf notneeded')
for f in files_to_sum:
	commands.append('rm '+f) 
	commands.append('hadd ' + f + " " + f.replace('_PSET','_job*_PSET') )
	commands.append('mv ' +  f.replace('_PSET','_job*_PSET') + ' temprootfiles/')
	#commands.append('mv ' +  f + ' rootfiles/')


commands.append('rm rootfiles/TBkinematicsttbar_PSET_'+cuts+'.root') #removes old file with same name in /rootfiles/
commands.append('python HistoWeight.py -i TBkinematicsttbar_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_'+cuts+'.root -o rootfiles/TBkinematicsttbar_PSET_'+cuts+'weighted.root -w ' + str(lumi*xsec_ttbar['MG']*ttagsf/nev_ttbar['MG']))
commands.append('mv TBkinematicsttbar_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_'+cuts+'.root temprootfiles/')

ptarray = [300, 470, 600, 800, 1000, 1400]

commands.append('rm ' + 'TBkinematicsQCDPT_PSET_'+cuts+'weighted.root')
commands.append('rm ' + 'TBkinematicsQCDPT_PSET_'+cuts+'.root')
commands.append('hadd ' + 'TBkinematicsQCD_PSET_'+cuts+'.root' + " " +'TBkinematicsQCDPT*_PSET_'+cuts+'.root')	#adds the separated pt files into one

for pti in ptarray:
	pt = str(pti)
	commands.append('rm ' + 'TBkinematicsQCDPT'+pt+'_PSET_'+cuts+'weighted.root')	#remove old weighted pt file
	commands.append('python HistoWeight.py -i TBkinematicsQCDPT'+pt+'_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_'+cuts+'.root -o TBkinematicsQCDPT'+pt+'_PSET_'+cuts+'weighted.root -w ' + str(lumi*xsec_qcd[pt]*ttagsf/nev_qcd[pt])) #weights individual pt files by their appropriate weight
	
commands.append('hadd ' + 'TBkinematicsQCD_PSET_'+cuts+'weighted.root' + " " + 'TBkinematicsQCDPT*_PSET_'+cuts+'weighted.root') #adds the separated weighted files together
commands.append('mv ' + 'TBkinematicsQCDPT*_PSET_'+cuts+'.root' + " " + 'temprootfiles/')		#moves the individual pt files to temp
commands.append('mv ' + 'TBkinematicsQCDPT*_PSET_'+cuts+'weighted.root' + " " + 'temprootfiles/')	#moves the invididual weighted pt files to temp

commands.append('rm ' + 'temprootfiles/TBkinematicsQCD_PSET_'+cuts+'.root')
commands.append('rm ' + 'rootfiles/TBkinematicsQCD_PSET_'+cuts+'weighted.root')
commands.append('mv ' + 'TBkinematicsQCD_PSET_'+cuts+'.root' + " " + 'temprootfiles/')
commands.append('mv ' + 'TBkinematicsQCD_PSET_'+cuts+'weighted.root' + " " + 'rootfiles/')


for coup in ['right','left','mixed']:
	sigfiles = sorted(glob.glob('TBkinematicssignal'+coup+'*_PSET_'+cuts+'.root'))
	for f in sigfiles:
		mass = f.replace('TBkinematicssignal'+coup,'')[:4]
		
		if coup =='right':
			xsec_sig = xsec_wpr[mass]
			nev_sig = nev_wpr[mass]
		if coup =='left':
			xsec_sig = xsec_wpl[mass]
			nev_sig = nev_wpl[mass]
		if coup =='mixed':
			xsec_sig = xsec_wplr[mass]
			nev_sig = nev_wplr[mass]
		commands.append('rm ' + f.replace('TBkinematicssignal'+coup,'TBkinematicsweightedsignal'+coup))	 
		commands.append('python HistoWeight.py -i '+f+' -o '+f.replace('TBkinematicssignal'+coup,'TBkinematicsweightedsignal'+coup)+' -w ' + str(lumi*xsec_sig*ttagsf/nev_sig))
		commands.append('mv '+f+' temprootfiles/')
		commands.append('mv '+f.replace('TBkinematicssignal'+coup,'TBkinematicsweightedsignal'+coup)+' rootfiles/')


stfiles = sorted(glob.glob('TBkinematicssingletop_*_Trigger_nominal_none_PSET_'+cuts+'.root'))

for f in stfiles:
	print f
	channel = f.replace('TBkinematicssingletop_','').replace('_Trigger_nominal_none_PSET_'+cuts+'.root','')
	print channel
	xsec_ST = xsec_st[channel]
	nev_ST = nev_st[channel]
	commands.append('rm ' + f.replace('TBkinematicssingletop_','TBkinematicsweightedsingletop_'))	 
	commands.append('python HistoWeight.py -i '+f+' -o '+f.replace('TBkinematicssingletop_','TBkinematicsweightedsingletop_')+' -w ' + str(lumi*xsec_ST*ttagsf/nev_ST))
	commands.append('mv '+f+' temprootfiles/')
	commands.append('mv '+f.replace('TBkinematicssingletop_','TBkinematicsweightedsingletop_')+' rootfiles/')



for s in commands :
    print 'executing ' + s
    subprocess.call( [s], shell=True )







