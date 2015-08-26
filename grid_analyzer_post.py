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

#ttbarfiles = sorted(glob.glob('TBanalyzerttbar700*_PSET_'+cuts+'.root'))
#for f in ttbarfiles:
#	basename = f.replace('700','')
#	name700 = f
#	name1000 = f.replace('700','1000')

#	scalestr = ''

#	if name700.find('ttbar700scaleup') != -1:
#		scalestr = 'scaleup'
#	if name700.find('ttbar700scaledown') != -1:
#		scalestr = 'scaledown'

#	commands.append('rm ' + basename)
#	commands.append('python HistoWeight.py -i '+name700+' -o temprootfiles/'+name700.replace('.root','')+'weighted.root -w ' + str(lumi*xsec_ttbar['700']*ttagsf/nev_ttbar['700'+scalestr]))
#	commands.append('python HistoWeight.py -i '+name1000+' -o temprootfiles/'+name1000.replace('.root','')+'weighted.root -w ' + str(lumi*xsec_ttbar['1000']*ttagsf/nev_ttbar['1000'+scalestr]))
#	commands.append('hadd '+basename+' temprootfiles/'+name700.replace('.root','')+'weighted.root temprootfiles/'+name1000.replace('.root','')+'weighted.root')
#	commands.append('mv ' + name700 + ' ' + name1000 + ' temprootfiles/')
#	commands.append('mv ' + basename + ' rootfiles/')

commands.append('rm rootfiles/TBanalyzerttbar_PSET_'+cuts+'.root') #removes old file with same name in /rootfiles/
commands.append('python HistoWeight.py -i TBanalyzerttbar_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_'+cuts+'.root -o rootfiles/TBanalyzerttbar_PSET_'+cuts+'weighted.root -w ' + str(lumi*xsec_ttbar['MG']*ttagsf/nev_ttbar['MG']))
commands.append('mv TBanalyzerttbar_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_'+cuts+'.root temprootfiles/')

ptarray = [300, 470, 600, 800, 1000, 1400]

commands.append('rm ' + 'TBanalyzerQCDPT_PSET_'+cuts+'weighted.root')
commands.append('rm ' + 'TBanalyzerQCDPT_PSET_'+cuts+'.root')
commands.append('hadd ' + 'TBanalyzerQCD_PSET_'+cuts+'.root' + " " +'TBanalyzerQCDPT*_PSET_'+cuts+'.root')	#adds the separated pt files into one

for pti in ptarray:
	pt = str(pti)
	commands.append('rm ' + 'TBanalyzerQCDPT'+pt+'_PSET_'+cuts+'weighted.root')	#remove old weighted pt file
	commands.append('python HistoWeight.py -i TBanalyzerQCDPT'+pt+'_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_'+cuts+'.root -o TBanalyzerQCDPT'+pt+'_PSET_'+cuts+'weighted.root -w ' + str(lumi*xsec_qcd[pt]*ttagsf/nev_qcd[pt])) #weights individual pt files by their appropriate weight
	
commands.append('hadd ' + 'TBanalyzerQCD_PSET_'+cuts+'weighted.root' + " " + 'TBanalyzerQCDPT*_PSET_'+cuts+'weighted.root') #adds the separated weighted files together
commands.append('mv ' + 'TBanalyzerQCDPT*_PSET_'+cuts+'.root' + " " + 'temprootfiles/')		#moves the individual pt files to temp
commands.append('mv ' + 'TBanalyzerQCDPT*_PSET_'+cuts+'weighted.root' + " " + 'temprootfiles/')	#moves the invididual weighted pt files to temp

commands.append('rm ' + 'temprootfiles/TBanalyzerQCD_PSET_'+cuts+'.root')
commands.append('rm ' + 'rootfiles/TBanalyzerQCD_PSET_'+cuts+'weighted.root')
commands.append('mv ' + 'TBanalyzerQCD_PSET_'+cuts+'.root' + " " + 'temprootfiles/')
commands.append('mv ' + 'TBanalyzerQCD_PSET_'+cuts+'weighted.root' + " " + 'rootfiles/')

for coup in ['right','left','mixed']:
	sigfiles = sorted(glob.glob('TBanalyzersignal'+coup+'*_PSET_'+cuts+'.root'))
	for f in sigfiles:
		mass = f.replace('TBanalyzersignal'+coup,'')[:4]
		
		if coup =='right':
			xsec_sig = xsec_wpr[mass]
			nev_sig = nev_wpr[mass]
		if coup =='left':
			xsec_sig = xsec_wpl[mass]
			nev_sig = nev_wpl[mass]
		if coup =='mixed':
			xsec_sig = xsec_wplr[mass]
			nev_sig = nev_wplr[mass]
		commands.append('rm ' + f.replace('TBanalyzersignal'+coup,'TBanalyzerweightedsignal'+coup))	 
		commands.append('python HistoWeight.py -i '+f+' -o '+f.replace('TBanalyzersignal'+coup,'TBanalyzerweightedsignal'+coup)+' -w ' + str(lumi*xsec_sig*ttagsf/nev_sig))
		commands.append('mv '+f+' temprootfiles/')
		commands.append('mv '+f.replace('TBanalyzersignal'+coup,'TBanalyzerweightedsignal'+coup)+' rootfiles/')


#rstfiles = sorted(glob.glob('TBanalyzersingletop_*_Trigger_nominal_none_PSET_'+cuts+'.root'))

#for f in stfiles:
#	print f
#	channel = f.replace('TBanalyzersingletop_','').replace('_Trigger_nominal_none_PSET_'+cuts+'.root','')
#	print channel
#	xsec_ST = xsec_st[channel]
#	nev_ST = nev_st[channel]
#	commands.append('rm ' + f.replace('TBanalyzersingletop_','TBanalyzerweightedsingletop_'))	 
#	commands.append('python HistoWeight.py -i '+f+' -o '+f.replace('TBanalyzersingletop_','TBanalyzerweightedsingletop_')+' -w ' + str(lumi*xsec_ST*ttagsf/nev_ST))
#	commands.append('mv '+f+' temprootfiles/')
#	commands.append('mv '+f.replace('TBanalyzersingletop_','TBanalyzerweightedsingletop_')+' rootfiles/')



for s in commands :
    print 'executing ' + s
    subprocess.call( [s], shell=True )







