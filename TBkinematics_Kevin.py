#! /usr/bin/env python

###################################################################
##								 ##
## Name: TBanalyzer.py	   			                 ##
## Author: Kevin Nash 						 ##
## Date: 6/5/2012						 ##
## Purpose: This program performs the main analysis.  		 ##
##	    It takes the tagrates created by  	 		 ##
##          TBrate_Maker.py stored in fitdata, and uses 	 ##
##          them to weigh pre b tagged samples to create a 	 ##
##	    QCD background estimate along with the full event    ##
##	    selection to product Mtb inputs to Theta		 ##
##								 ##
###################################################################

import os
import glob
import math
from math import sqrt
#import quickroot
#from quickroot import *
import ROOT 
from ROOT import *

import sys
from DataFormats.FWLite import Events, Handle
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-s', '--set', metavar='F', type='string', action='store',
                  default	=	'data',
                  dest		=	'set',
                  help		=	'data or ttbar')
parser.add_option('-x', '--pileup', metavar='F', type='string', action='store',
                  default	=	'on',
                  dest		=	'pileup',
                  help		=	'If not data do pileup reweighting?')
parser.add_option('-n', '--num', metavar='F', type='string', action='store',
                  default	=	'all',
                  dest		=	'num',
                  help		=	'job number')
parser.add_option('-j', '--jobs', metavar='F', type='string', action='store',
                  default	=	'1',
                  dest		=	'jobs',
                  help		=	'number of jobs')
parser.add_option('-t', '--tname', metavar='F', type='string', action='store',
                  default	=	'HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1',
                  dest		=	'tname',
                  help		=	'trigger name')

parser.add_option('-m', '--modulesuffix', metavar='F', type='string', action='store',
                  default	=	'none',
                  dest		=	'modulesuffix',
                  help		=	'ex. PtSmearUp')

parser.add_option('-g', '--grid', metavar='F', type='string', action='store',
                  default	=	'off',
                  dest		=	'grid',
                  help		=	'running on grid off or on')
parser.add_option('-u', '--ptreweight', metavar='F', type='string', action='store',
                  default	=	'none',
                  dest		=	'ptreweight',
                  help		=	'on or off')

parser.add_option('-p', '--pdfweights', metavar='F', type='string', action='store',
                  default	=	'nominal',
                  dest		=	'pdfweights',
                  help		=	'nominal, up, or down')
parser.add_option('-z', '--pdfset', metavar='F', type='string', action='store',
                  default	=	'cteq66',
                  dest		=	'pdfset',
                  help		=	'pdf set')
parser.add_option('--printEvents', metavar='F', action='store_true',
                  default=False,
                  dest='printEvents',
                  help='Print events that pass selection (run:lumi:event)')
parser.add_option('-c', '--cuts', metavar='F', type='string', action='store',
                  default	=	'default',
                  dest		=	'cuts',
                  help		=	'Cuts type (ie default, rate, etc)')

parser.add_option('-b', '--bx', metavar='F', type='string', action='store',
                  default	=	'25ns',
                  dest		=	'bx',
                  help		=	'bunch crossing 50ns or 25ns')




(options, args) = parser.parse_args()

tname = options.tname.split(',')
tnamestr = ''
for iname in range(0,len(tname)):
	tnamestr+=tname[iname]
	if iname!=len(tname)-1:
		tnamestr+='OR'
trig='none'
if options.set!= 'data': 
	if options.tname=='HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1':
		trig = 'nominal'
	elif options.tname!= []:
		trig = 'tnamestr'
		

print "Options summary"
print "=================="
for  opt,value in options.__dict__.items():
	#print str(option)+ ": " + str(options[option]) 
	print str(opt) +': '+ str(value)
print "=================="
print ""
di = ""
if options.grid == 'on':
	di = "tardir/"
	sys.path.insert(0, 'tardir/')



import Wprime_Functions	
from Wprime_Functions import *

#Load up cut values based on what selection we want to run 
Cuts = LoadCuts(options.cuts)
bpt = Cuts['bpt']
tpt = Cuts['tpt']
dy = Cuts['dy']
tmass = Cuts['tmass']
nsubjets = Cuts['nsubjets']
tau32 = Cuts['tau32']
minmass = Cuts['minmass']
sjbtag = Cuts['sjbtag']
bmass = Cuts['bmass']
btag = Cuts['btag']
eta1 = Cuts['eta1']
eta2 = Cuts['eta2']
eta3 = Cuts['eta3']

#For large datasets we need to parallelize the processing
jobs=int(options.jobs)
if jobs != 1:
	num=int(options.num)
	jobs=int(options.jobs)
	print "Running over " +str(jobs)+ " jobs"
	print "This will process job " +str(num)
else:
	print "Running over all events"

#This section defines some strings that are used in naming the optput files
mod = "ttbsmAna"
if options.modulesuffix != "none" :
	mod = mod + options.modulesuffix

pstr = ""
if options.pdfweights!="nominal":
	print "using pdf uncertainty"
	pstr = "_pdf_"+options.pdfset+"_"+options.pdfweights

pustr = ""
if options.pileup=='off':
	pustr = "_pileup_unweighted"


run_b_SF = True
#Based on what set we want to analyze, we find all Ntuple root files 

files = Load_Ntuples(options.set,options.bx)

if (options.set.find('ttbar') != -1) or (options.set.find('singletop') != -1):
	settype = 'ttbar'
elif (options.set.find('QCD') != -1):
	settype ='QCD'
	run_b_SF = False
else :
	
	settype = options.set.replace('right','').replace('left','').replace('mixed','')

print 'The type of set is ' + settype

if options.set != 'data':
	#Load up scale factors (to be used for MC only)

	#TrigFile = TFile(di+"TRIG_EFFICWPHTdata_dijet8TeV.root")
	#TrigPlot = TrigFile.Get("r11")

	TrigFile = TFile(di+"Triggerweight_signalright2000.root")
	TrigPlot = TrigFile.Get("TriggerWeight_"+tnamestr)

	#PileFile = TFile(di+"PileUp_Ratio_"+settype+".root")
	#PilePlot = PileFile.Get("Pileup_Ratio")

# We select all the events:    
events = Events (files)

#Here we load up handles and labels.
#These are used to grab entries from the Ntuples.
#To see all the current types in an Ntuple use edmDumpEventContent /PathtoNtuple/Ntuple.root

AK8HL = Initlv("jetsAK8")
	
BDiscHandle 	= 	Handle (  "vector<float> "  )
BDiscLabel  	= 	( "jetsAK8" , "jetAK8CSV")

GeneratorHandle 	= 	Handle (  "GenEventInfoProduct")
GeneratorLabel  	= 	( "generator" , "")

puHandle    	= 	Handle("int")
puLabel     	= 	( "eventUserData", "puNtrueInt" )

minmassHandle 	= 	Handle (  "vector<float> "  )
minmassLabel  	= 	( "jetsAK8" , "jetAK8minmass")

nSubjetsHandle 	= 	Handle (  "vector<float> "  )
nSubjetsLabel  	= 	( "jetsAK8" , "jetAK8nSubJets")



AK8MassHandle 	= 	Handle (  "vector<float> "  )
AK8MassLabel  	= 	( "jetsAK8" , "jetAK8Mass")

prunedMassHandle 	= 	Handle (  "vector<float> "  )
prunedMassLabel  	= 	( "jetsAK8" , "jetAK8prunedMass")

filteredMassHandle 	= 	Handle (  "vector<float> "  )
filteredMassLabel  	= 	( "jetsAK8" , "jetAK8filteredMass")

topMassHandle 	= 	Handle (  "vector<float> "  )
topMassLabel  	= 	( "jetsAK8" , "jetAK8topMass")

trimmedMassHandle 	= 	Handle (  "vector<float> "  )
trimmedMassLabel  	= 	( "jetsAK8" , "jetAK8trimmedMass")

softDropMassHandle 	= 	Handle (  "vector<float> "  )
softDropMassLabel  	= 	( "jetsAK8" , "jetAK8softDropMass")

tau1Handle 	= 	Handle (  "vector<float> "  )
tau1Label  	= 	( "jetsAK8" , "jetAK8tau1")

tau2Handle 	= 	Handle (  "vector<float> "  )
tau2Label  	= 	( "jetsAK8" , "jetAK8tau2")

tau3Handle 	= 	Handle (  "vector<float> "  )
tau3Label  	= 	( "jetsAK8" , "jetAK8tau3")

topMassHandle 	= 	Handle (  "vector<float> "  )
topMassLabel  	= 	( "jetsAK8" , "jetAK8topMass")

subjetsCSVHandle 	= 	Handle (  "vector<float> "  )
subjetsCSVLabel  	= 	( "subjetsCmsTopTag" , "subjetCmsTopTagCSV")

subjets0indexHandle 	= 	Handle (  "vector<float> "  )
subjets0indexLabel  	= 	( "jetsAK8" , "jetAK8topSubjetIndex0")

subjets1indexHandle 	= 	Handle (  "vector<float> "  )
subjets1indexLabel  	= 	( "jetsAK8" , "jetAK8topSubjetIndex1")

subjets2indexHandle 	= 	Handle (  "vector<float> "  )
subjets2indexLabel  	= 	( "jetsAK8" , "jetAK8topSubjetIndex2")

subjets3indexHandle 	= 	Handle (  "vector<float> "  )
subjets3indexLabel  	= 	( "jetsAK8" , "jetAK8topSubjetIndex3")

#---------------------------------------------------------------------------------------------------------------------#

if jobs != 1:
	f = TFile( "TBkinematics"+options.set+"_Trigger_"+trig+"_"+options.modulesuffix +pustr+pstr+"_job"+options.num+"of"+options.jobs+"_PSET_"+options.cuts+".root", "recreate" )
else:
	f = TFile( "TBkinematics"+options.set+"_Trigger_"+trig+"_"+options.modulesuffix +pustr+pstr+"_PSET_"+options.cuts+".root", "recreate" )


#Load up the average b-tagging rates -- Takes parameters from text file and makes a function
BTR = BTR_Init('Bifpoly','rate_'+options.cuts,di)
BTR_err = BTR_Init('Bifpoly_err','rate_'+options.cuts,di)
fittitles = ["pol0","pol2","pol3","FIT","Bifpoly","expofit"]
fits = []
for fittitle in fittitles:
	fits.append(BTR_Init(fittitle,'rate_'+options.cuts,di))

print "Creating histograms"

#Define Histograms

TagFile1 = TFile(di+"Tagrate2D.root")
TagPlot2de1= TagFile1.Get("tagrateeta1")
TagPlot2de2= TagFile1.Get("tagrateeta2")
TagPlot2de3= TagFile1.Get("tagrateeta3")

f.cd()
#---------------------------------------------------------------------------------------------------------------------#
Nevents	    = TH1F("Nevents",     	  "mass of tb",     	  	         5, 0., 5. )

Mtb1	    = TH1F("Mtb1",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb2	    = TH1F("Mtb2",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb3	    = TH1F("Mtb3",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb4	    = TH1F("Mtb4",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb5	    = TH1F("Mtb5",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb6	    = TH1F("Mtb6",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb7	    = TH1F("Mtb7",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb8	    = TH1F("Mtb8",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb9	    = TH1F("Mtb9",     	  "mass of tb",     	  	         140, 500, 4000 )
Mtb10	    = TH1F("Mtb10",    	  "mass of tb",     	  	         140, 500, 4000 )

HG_njets	    = TH1F("HG_njets",     "nsubjets in b+1",                   7, -0.5, 6.5 )

HG_teta        	= TH1F("HG_teta",      "top eta in b+1",                   24, -2.4, 2.4 )
HG_beta        	= TH1F("HG_beta",      "b eta in b+1",                     24, -2.4, 2.4 )
HG_tpt         	= TH1F("HG_tpt",       "top pt in b+1",                   150, 0, 1500 )
HG_bpt         	= TH1F("HG_bpt",       "b pt in b+1",                     150, 0, 1500 )
HG_sumpt       	= TH1F("HG_sumpt",     "dijet lorentz sum pt in b+1",     150, 0, 1000 )
HG_tphi        	= TH1F("HG_tphi",      "t phi in b+1",                     24, -3.14, 3.14 )
HG_bphi        	= TH1F("HG_bphi",      "b phi in b+1",                     24, -3.14, 3.14 )
HG_dphi 	= TH1F("HG_dphi",      "delta phi in b+1",                75, 0.0, 6.28 )
HG_csv 	= TH1F("HG_csv",      "delta phi in b+1",                100, 0.0, 1.0 )

HG_drap        = TH1F("HG_drap",      "delta rapidity in b+1",            24, 0, 5 )
HG_drap_highmtb       = TH1F("HG_drap_highmtb",      "delta rapidity in b+1",            24, 0, 5 )

HG_TopBDiscsjmaxCSV = TH1F("HG_TopBDiscsjmaxCSV",  "top mass in b+1",    100, 0, 1.0 )

HG_tau32       = TH1F("HG_tau32",     "top mass in b+1",                 100, 0., 1.5 )
HG_bmass	    = TH1F("HG_bmass",     "b mass in b+1",                   150, 0, 300 )
HG_tmass       = TH1F("HG_tmass",     "top mass in b+1",                 150, 0, 500 )

HG_tmassAK8       = TH1F("HG_tmassAK8",     "top mass in b+1",                 150, 0, 500 )
HG_tmasspruned       = TH1F("HG_tmasspruned",     "top mass in b+1",                 150, 0, 500 )
HG_tmassfiltered       = TH1F("HG_tmassfiltered",     "top mass in b+1",                 150, 0, 500 )
HG_tmasstopMass       = TH1F("HG_tmasstopMass",     "top mass in b+1",                 150, 0, 500 )
HG_tmasstrimmed       = TH1F("HG_tmasstrimmed",     "top mass in b+1",                 150, 0, 500 )
HG_tmassjetmass       = TH1F("HG_tmassjetmass",     "top mass in b+1",                 150, 0, 500 )
HG_tmasssoftdrop       = TH1F("HG_tmasssoftdrop",     "top mass in b+1",                 150, 0, 500 )

HG_tmassAK8post       = TH1F("HG_tmassAK8post",     "top mass in b+1",                 150, 0, 500 )
HG_tmassprunedpost       = TH1F("HG_tmassprunedpost",     "top mass in b+1",                 150, 0, 500 )
HG_tmassfilteredpost       = TH1F("HG_tmassfilteredpost",     "top mass in b+1",                 150, 0, 500 )
HG_tmasstopMasspost       = TH1F("HG_tmasstopMasspost",     "top mass in b+1",                 150, 0, 500 )
HG_tmasstrimmedpost       = TH1F("HG_tmasstrimmedpost",     "top mass in b+1",                 150, 0, 500 )
HG_tmassjetmasspost       = TH1F("HG_tmassjetmasspost",     "top mass in b+1",                 150, 0, 500 )
HG_tmasssoftdroppost       = TH1F("HG_tmasssoftdroppost",     "top mass in b+1",                 150, 0, 500 )

HG_bmassAK8       = TH1F("HG_bmassAK8",     "top mass in b+1",                 150, 0, 500 )
HG_bmasspruned       = TH1F("HG_bmasspruned",     "top mass in b+1",                 150, 0, 500 )
HG_bmassfiltered       = TH1F("HG_bmassfiltered",     "top mass in b+1",                 150, 0, 500 )
HG_bmasstopMass       = TH1F("HG_bmasstopMass",     "top mass in b+1",                 150, 0, 500 )
HG_bmasstrimmed       = TH1F("HG_bmasstrimmed",     "top mass in b+1",                 150, 0, 500 )
HG_bmassjetmass       = TH1F("HG_bmassjetmass",     "top mass in b+1",                 150, 0, 500 )



HG_minmass     = TH1F("HG_minmass",   "minmass in b+1",                   60, 0, 120 )
HG_nsubjets    = TH1F("HG_nsubjets",  "nsubjets in b+1",                   7, -0.5, 6.5 )
#-------------QCD bkg error for each parameter---------#

QCDbkg 	    	= TH1F("QCDbkg",     "QCD background estimate",     	 140, 500, 4000 )
QCDbkgh	    	= TH1F("QCDbkgh",    "QCD background estimate up error",     140, 500, 4000 )
QCDbkgl     	= TH1F("QCDbkgl",    "QCD background estimate down error",   140, 500, 4000 )
QCDbkg2D    	= TH1F("QCDbkg2D",   "QCD background estimate 2d error",     140, 500, 4000 )

QCDbkg_tmass 	= TH1F("QCDbkg_tmass",   "QCD bkg estimate for top mass in b+1",   	  	150, 0, 500 )
QCDbkgh_tmass 	= TH1F("QCDbkgh_tmass",  "QCD bkg estimate up error for top mass in b+1", 	150, 0, 500 )
QCDbkgl_tmass 	= TH1F("QCDbkgl_tmass",  "QCD bkg estimate down error for top mass in b+1",	150, 0, 500 )
QCDbkg2D_tmass 	= TH1F("QCDbkg2D_tmass", "QCD bkg estimate for top mass in b+1",   		150, 0, 500 )

QCDbkg_teta 	= TH1F("QCDbkg_teta",   "QCD bkg estimate for top eta in b+1",   	  	24, -2.4, 2.4 )
QCDbkgh_teta 	= TH1F("QCDbkgh_teta",  "QCD bkg estimate up error for top eta in b+1", 	24, -2.4, 2.4 )
QCDbkgl_teta 	= TH1F("QCDbkgl_teta",  "QCD bkg estimate down error for top eta in b+1",	24, -2.4, 2.4 )
QCDbkg2D_teta 	= TH1F("QCDbkg2D_teta", "QCD bkg estimate for top eta in b+1",   		24, -2.4, 2.4 )

QCDbkg_beta 	= TH1F("QCDbkg_beta",   "QCD bkg estimate for b eta in b+1",   	  	24, -2.4, 2.4 )
QCDbkgh_beta 	= TH1F("QCDbkgh_beta",  "QCD bkg estimate up error for b eta in b+1", 	24, -2.4, 2.4 )
QCDbkgl_beta 	= TH1F("QCDbkgl_beta",  "QCD bkg estimate down error for b eta in b+1",	24, -2.4, 2.4 )
QCDbkg2D_beta 	= TH1F("QCDbkg2D_beta", "QCD bkg estimate for b eta in b+1",   		24, -2.4, 2.4 )

QCDbkg_tpt 	= TH1F("QCDbkg_tpt",   "QCD bkg estimate for top pt in b+1",   	  	150, 0, 1500 )
QCDbkgh_tpt 	= TH1F("QCDbkgh_tpt",  "QCD bkg estimate up error for top pt in b+1", 	150, 0, 1500 )
QCDbkgl_tpt 	= TH1F("QCDbkgl_tpt",  "QCD bkg estimate down error for top pt in b+1",	150, 0, 1500 )
QCDbkg2D_tpt 	= TH1F("QCDbkg2D_tpt", "QCD bkg estimate for top pt in b+1",   		150, 0, 1500 )

QCDbkg_bpt 	= TH1F("QCDbkg_bpt",   "QCD bkg estimate for b pt in b+1",   	  	150, 0, 1500 )
QCDbkgh_bpt 	= TH1F("QCDbkgh_bpt",  "QCD bkg estimate up error for b pt in b+1", 	150, 0, 1500 )
QCDbkgl_bpt 	= TH1F("QCDbkgl_bpt",  "QCD bkg estimate down error for b pt in b+1",	150, 0, 1500 )
QCDbkg2D_bpt 	= TH1F("QCDbkg2D_bpt", "QCD bkg estimate for b pt in b+1",   		150, 0, 1500 )

QCDbkg_sumpt 	= TH1F("QCDbkg_sumpt",   "QCD bkg estimate for sum pt in b+1",   	  	150, 0, 1500 )
QCDbkgh_sumpt 	= TH1F("QCDbkgh_sumpt",  "QCD bkg estimate up error for sum pt in b+1", 	150, 0, 1500 )
QCDbkgl_sumpt 	= TH1F("QCDbkgl_sumpt",  "QCD bkg estimate down error for sum pt in b+1",	150, 0, 1500 )
QCDbkg2D_sumpt 	= TH1F("QCDbkg2D_sumpt", "QCD bkg estimate for sum pt in b+1",   		150, 0, 1500 )

QCDbkg_tphi 	= TH1F("QCDbkg_tphi",   "QCD bkg estimate for t phi in b+1",   	  	24, -3.14, 3.14 )
QCDbkgh_tphi 	= TH1F("QCDbkgh_tphi",  "QCD bkg estimate up error for t phi in b+1", 	24, -3.14, 3.14 )
QCDbkgl_tphi 	= TH1F("QCDbkgl_tphi",  "QCD bkg estimate down error for t phi in b+1",	24, -3.14, 3.14 )
QCDbkg2D_tphi 	= TH1F("QCDbkg2D_tphi", "QCD bkg estimate for t phi in b+1",   		24, -3.14, 3.14 )

QCDbkg_bphi 	= TH1F("QCDbkg_bphi",   "QCD bkg estimate for b phi in b+1",   	  	24, -3.14, 3.14 )
QCDbkgh_bphi 	= TH1F("QCDbkgh_bphi",  "QCD bkg estimate up error for b phi in b+1", 	24, -3.14, 3.14 )
QCDbkgl_bphi 	= TH1F("QCDbkgl_bphi",  "QCD bkg estimate down error for b phi in b+1",	24, -3.14, 3.14 )
QCDbkg2D_bphi 	= TH1F("QCDbkg2D_bphi", "QCD bkg estimate for b phi in b+1",   		24, -3.14, 3.14 )

QCDbkg_dphi 	= TH1F("QCDbkg_dphi",   "QCD bkg estimate for d phi in b+1",   	  	150, -6.28, 6.28 )
QCDbkgh_dphi 	= TH1F("QCDbkgh_dphi",  "QCD bkg estimate up error for d phi in b+1", 	150, -6.28, 6.28 )
QCDbkgl_dphi 	= TH1F("QCDbkgl_dphi",  "QCD bkg estimate down error for d phi in b+1",	150, -6.28, 6.28 )
QCDbkg2D_dphi 	= TH1F("QCDbkg2D_dphi", "QCD bkg estimate for d phi in b+1",   		150, -6.28, 6.28 )

Mtb1.Sumw2()
Mtb2.Sumw2()
Mtb3.Sumw2()
Mtb4.Sumw2()
Mtb5.Sumw2()
Mtb6.Sumw2()
Mtb7.Sumw2()
Mtb8.Sumw2()
Mtb9.Sumw2()
Mtb10.Sumw2()
#Mtbptup.Sumw2()
#Mtbptdown.Sumw2()
#MtbBup.Sumw2()
#MtbBDown.Sumw2()

HG_csv.Sumw2()
HG_tmass.Sumw2()
HG_minmass.Sumw2()
HG_nsubjets.Sumw2()
HG_njets.Sumw2()
HG_teta.Sumw2()
HG_beta.Sumw2()
HG_tpt.Sumw2()
HG_bpt.Sumw2()
HG_sumpt.Sumw2()
HG_tphi.Sumw2()
HG_bphi.Sumw2()
HG_drap.Sumw2()
HG_drap_highmtb.Sumw2()
HG_dphi.Sumw2()
HG_TopBDiscsjmaxCSV.Sumw2()
HG_tau32.Sumw2()
HG_bmass.Sumw2()

QCDbkg.Sumw2()
QCDbkg_tmass.Sumw2()
QCDbkg_teta.Sumw2()
QCDbkg_beta.Sumw2()
QCDbkg_tpt.Sumw2()
QCDbkg_bpt.Sumw2()
QCDbkg_sumpt.Sumw2()
QCDbkg_tphi.Sumw2()
QCDbkg_bphi.Sumw2()
QCDbkg_dphi.Sumw2()


HG_tmassAK8post.Sumw2()
HG_tmassprunedpost.Sumw2()
HG_tmassfilteredpost.Sumw2()
HG_tmasstopMasspost.Sumw2()
HG_tmasstrimmedpost.Sumw2()
HG_tmassjetmasspost.Sumw2()
HG_tmasssoftdroppost.Sumw2()

HG_tmassAK8.Sumw2()
HG_tmasspruned.Sumw2()
HG_tmassfiltered.Sumw2()
HG_tmasstopMass.Sumw2()
HG_tmasstrimmed.Sumw2()
HG_tmassjetmass.Sumw2()
HG_tmasssoftdrop.Sumw2()

HG_bmassAK8.Sumw2()
HG_bmasspruned.Sumw2()
HG_bmassfiltered.Sumw2()
HG_bmasstopMass.Sumw2()
HG_bmasstrimmed.Sumw2()
HG_bmassjetmass.Sumw2()


QCDbkg_ARR 	 = []
QCDbkg_tmass_ARR = []
QCDbkg_teta_ARR  = []
QCDbkg_beta_ARR  = []
QCDbkg_tpt_ARR   = []
QCDbkg_bpt_ARR   = []
QCDbkg_sumpt_ARR = []
QCDbkg_tphi_ARR  = []
QCDbkg_bphi_ARR  = []
QCDbkg_dphi_ARR  = []


for ihist in range(0,len(fittitles)):
	QCDbkg_ARR.append(TH1F("QCDbkg"+str(fittitles[ihist]),     "mass W' in b+1 pt est etabin",     	  	      140, 500, 4000 ))
	QCDbkg_tmass_ARR.append(TH1F("QCDbkg_tmass"+str(fittitles[ihist]),     "top mass in b+1",                	 150, 0, 500 ))
	QCDbkg_teta_ARR.append(TH1F("QCDbkg_teta"+str(fittitles[ihist]),     "top eta in b+1",				24, -2.4, 2.4 ))
	QCDbkg_beta_ARR.append(TH1F("QCDbkg_beta"+str(fittitles[ihist]),     "b eta in b+1",				24, -2.4, 2.4 ))
	QCDbkg_tpt_ARR.append(TH1F("QCDbkg_tpt"+str(fittitles[ihist]),     	"top pt in b+1",			150, 0, 1500 ))
	QCDbkg_bpt_ARR.append(TH1F("QCDbkg_bpt"+str(fittitles[ihist]),     	"b pt in b+1",       			150, 0, 1500 ))
	QCDbkg_sumpt_ARR.append(TH1F("QCDbkg_sumpt"+str(fittitles[ihist]),    "dijet lorentz sum pt in b+1",       	150, 0, 1500 ))
	QCDbkg_tphi_ARR.append(TH1F("QCDbkg_tphi"+str(fittitles[ihist]),     "t phi in b+1",				24, -3.14, 3.14 ))
	QCDbkg_bphi_ARR.append(TH1F("QCDbkg_bphi"+str(fittitles[ihist]),     "b phi in b+1",				24, -3.14, 3.14 ))
	QCDbkg_dphi_ARR.append(TH1F("QCDbkg_dphi"+str(fittitles[ihist]),     "delta eta in b+1",			150, -6.28, 6.28 ))
	
	QCDbkg_ARR[ihist].Sumw2()
	QCDbkg_tmass_ARR[ihist].Sumw2()
	QCDbkg_teta_ARR[ihist].Sumw2()
	QCDbkg_beta_ARR[ihist].Sumw2()
	QCDbkg_tpt_ARR[ihist].Sumw2()
	QCDbkg_bpt_ARR[ihist].Sumw2()
	QCDbkg_sumpt_ARR[ihist].Sumw2()
	QCDbkg_tphi_ARR[ihist].Sumw2()
	QCDbkg_bphi_ARR[ihist].Sumw2()
	QCDbkg_dphi_ARR[ihist].Sumw2()	
	




#---------------------------------------------------------------------------------------------------------------------#

# loop over events
#---------------------------------------------------------------------------------------------------------------------#

count = 0
jobiter = 0
print "Start looping"
#initialize the ttree variables
tree_vars = {"bpt":array('d',[0.]),"bmass":array('d',[0.]),"btag":array('d',[0.]),"tpt":array('d',[0.]),"tmass":array('d',[0.]),"nsubjets":array('d',[0.]),"sjbtag":array('d',[0.])}
Tree = Make_Trees(tree_vars)

goodEvents = []
totevents = events.size()
print str(totevents)  +  ' Events total'
usegenweight = False
if options.set == "QCDFLAT7000":
	usegenweight = True
	print "Using gen weight"


for event in events:



    weightSFb = 1.0
    errorSFb = 0.0
    count	= 	count + 1
    
    m = 0
    t = 0
    #if count > 1000:
	#break

    if count % 100000 == 0 :
      print  '--------- Processing Event ' + str(count) +'   -- percent complete ' + str(100*count/totevents) + '% -- '

    #Here we split up event processing based on number of jobs 
    #This is set up to have jobs range from 1 to the total number of jobs (ie dont start at job 0)

    if jobs != 1:
    	if (count - 1) % jobs == 0:
		jobiter+=1
	count_index = count - (jobiter-1)*jobs
	if count_index!=num:
		continue 

    if usegenweight:

		try:
			event.getByLabel (GeneratorLabel, GeneratorHandle)
    			gen 		= 	GeneratorHandle.product()
			Nevents.Fill(0.,gen.weightProduct())
		except:
			continue 

    # We load up the relevant handles and labels and create collections
    AK8LV = Makelv(AK8HL,event)

    if len(AK8LV)==0:
	continue

    tindex,bindex = Hemispherize(AK8LV,AK8LV)


    bJetsh1 = []
    bJetsh0  =  []
    topJetsh1 = []
    topJetsh0  = []

    for i in range(0,len(bindex[1])):
   	bJetsh1.append(AK8LV[bindex[1][i]])
    for i in range(0,len(bindex[0])):
    	bJetsh0.append(AK8LV[bindex[0][i]])
    for i in range(0,len(tindex[1])):
    	topJetsh1.append(AK8LV[tindex[1][i]])
    for i in range(0,len(tindex[0])):
    	topJetsh0.append(AK8LV[tindex[0][i]])
    
    bjh0 = 0
    bjh1 = 0

    #Require 1 pt>150 jet in each hemisphere (top jets already have the 150GeV requirement) 

    for bjet in bJetsh0:
	if bjet.Perp() > 200.0:
		bjh0+=1
    for bjet in bJetsh1:
	if bjet.Perp() > 200.0:
		bjh1+=1

    njets11b0 	= 	((len(topJetsh1) == 1) and (bjh0 == 1))
    njets11b1 	= 	((len(topJetsh0) == 1) and (bjh1 == 1))

    c1 = 0
    c2 = 0
    c3 = 0
    c4 = 0
    c5 = 0
    c6 = 0
    c7 = 0
    c8 = 0
    c9 = 0
    c10 = 0
	
    for hemis in ['hemis0','hemis1']:
    	if hemis == 'hemis0'   :
		if not njets11b0:
			continue 
		#The Ntuple entries are ordered in pt, so [0] is the highest pt entry
		#We are calling a candidate b jet (highest pt jet in hemisphere0)  

		tindexval = tindex[1][0]
		bindexval = bindex[0][0]

		bjet = bJetsh0[0]
		tjet = topJetsh1[0]


    	if hemis == 'hemis1'  :
		if not njets11b1:
			continue 

		tindexval = tindex[0][0]
		bindexval = bindex[1][0]

		bjet = bJetsh1[0]
		tjet = topJetsh0[0]


	if abs(bjet.Eta())>2.40 or abs(tjet.Eta())>2.40:
		continue 


    	weight=1.0

	ht = tjet.Perp() + bjet.Perp()
	if tname != [] and options.set!='data' :
				
		#Trigger reweighting done here
		TRW = Trigger_Lookup( ht , TrigPlot )
		weight*=TRW
		#print weight

	if usegenweight:
		try:
			weight*=gen.weightProduct()
		except:
			continue 
	#print gen.weightProduct()
	#print weight
	#print ""
	if False:# options.set!="data":

		event.getByLabel (puLabel, puHandle)
    		PileUp 		= 	puHandle.product()
               	bin1 = PilePlot.FindBin(PileUp[0]) 

		if options.pileup != 'off':
			weight *= PilePlot.GetBinContent(bin1)


		if run_b_SF :
			#btagging scale factor reweighting done here
			SFB = SFB_Lookup( bjet.Perp() )
			weightSFb = SFB[0]
			errorSFb = SFB[1]

    	bpt_cut = bpt[0]<bjet.Perp()<bpt[1]
    	tpt_cut = tpt[0]<tjet.Perp()<tpt[1]
    	dy_cut = dy[0]<=abs(tjet.Rapidity()-bjet.Rapidity())<dy[1]
        event.getByLabel (softDropMassLabel, softDropMassHandle)
        topJetMass 	= 	softDropMassHandle.product()
	tmass_cut = tmass[0]<topJetMass[tindexval]<tmass[1]



        event.getByLabel ( nSubjetsLabel , nSubjetsHandle )
    	nSubjets 		= 	nSubjetsHandle.product()
        event.getByLabel (minmassLabel, minmassHandle)
    	topJetminmass 	= 	minmassHandle.product()
	minmass_cut = minmass[0]<=topJetminmass[tindexval]<minmass[1]
	nsubjets_cut = nsubjets[0]<=nSubjets[tindexval]<nsubjets[1]


    	event.getByLabel (subjets0indexLabel, subjets0indexHandle)
    	subjets0index 		= 	subjets0indexHandle.product() 

    	event.getByLabel (subjets1indexLabel, subjets1indexHandle)
    	subjets1index 		= 	subjets1indexHandle.product() 

    	event.getByLabel (subjets2indexLabel, subjets2indexHandle)
    	subjets2index 		= 	subjets2indexHandle.product() 

    	event.getByLabel (subjets3indexLabel, subjets3indexHandle)
    	subjets3index 		= 	subjets3indexHandle.product()

    	event.getByLabel (subjetsCSVLabel, subjetsCSVHandle)
    	subjetsCSV 		= 	subjetsCSVHandle.product()  

	if nSubjets[tindexval]==0:
		continue 

	SJ_csvs = [subjets0index,subjets1index,subjets2index,subjets3index]
			
	SJ_csvvals = []

	for icsv in range(0,int(nSubjets[tindexval])):
		if int(SJ_csvs[icsv][tindexval])!=-1:
			SJ_csvvals.append(subjetsCSV[int(SJ_csvs[icsv][tindexval])])
		else:
			SJ_csvvals.append(0.)
	SJ_csvmax = max(SJ_csvvals)
	sjbtag_cut = sjbtag[0]<SJ_csvmax<=sjbtag[1]



    	event.getByLabel (tau1Label, tau1Handle)
    	tau1 		= 	tau1Handle.product()  


    	event.getByLabel (tau2Label, tau2Handle)
    	tau2 		= 	tau2Handle.product()  


    	event.getByLabel (tau3Label, tau3Handle)
    	tau3 		= 	tau3Handle.product()  


	tau32_cut =  tau32[0]<=tau3[tindexval]/tau2[tindexval]<tau32[1]

   	event.getByLabel (BDiscLabel, BDiscHandle)
    	bJetBDisc 	= 	BDiscHandle.product()
		
        btag_cut = btag[0]<bJetBDisc[bindexval]<=btag[1]
	bmass_cut = bmass[0]<=topJetMass[bindexval]<bmass[1]
	if c1==0:
		Mtb1.Fill((bjet+tjet).M(),weight)
		c1=1
	
    	fullsel =  bpt_cut and tpt_cut and dy_cut and tmass_cut and nsubjets_cut and  minmass_cut and sjbtag_cut and tau32_cut and bmass_cut and btag_cut

    	dysel =  bpt_cut and tpt_cut  and tmass_cut and nsubjets_cut and  minmass_cut  
    	tmasssel =  bpt_cut and tpt_cut and dy_cut  and nsubjets_cut and  minmass_cut   and bmass_cut and  btag_cut
    	tmassselpre =  bpt_cut and tpt_cut and dy_cut  and bmass_cut and  btag_cut
    	nsjsel =  bpt_cut and tpt_cut and dy_cut and tmass_cut   and bmass_cut and btag_cut
    	minmsel =  bpt_cut and tpt_cut and dy_cut and tmass_cut and nsubjets_cut   and bmass_cut and btag_cut 
    	sjbtagsel =  bpt_cut and tpt_cut and dy_cut and tmass_cut and nsubjets_cut and  minmass_cut   and bmass_cut and btag_cut 
    	t32sel =  bpt_cut and tpt_cut and dy_cut and tmass_cut and nsubjets_cut   and bmass_cut and btag_cut 
    	bmasssel =  bpt_cut and tpt_cut and dy_cut and tmass_cut and nsubjets_cut and  minmass_cut    and btag_cut 
    	btagsel =  bpt_cut and tpt_cut and dy_cut and tmass_cut and nsubjets_cut and  minmass_cut   and bmass_cut  

    	if bpt_cut and tpt_cut: 
		if c2==0:
			Mtb2.Fill((bjet+tjet).M(),weight)
			c2=1
 		if dy_cut:
			if c3==0:
				Mtb3.Fill((bjet+tjet).M(),weight)
				c3=1
			if tmass_cut:
				if c4==0:
					Mtb4.Fill((bjet+tjet).M(),weight)
					c4=1
				if nsubjets_cut:
					if c5==0:
						Mtb5.Fill((bjet+tjet).M(),weight)
						c5=1
	 				if minmass_cut:
						if c6==0:
							Mtb6.Fill((bjet+tjet).M(),weight)
							c6=1
						if sjbtag_cut :
							if c7==0:
								Mtb7.Fill((bjet+tjet).M(),weight)
								c7=1
							if tau32_cut:
								if c8==0:
									Mtb8.Fill((bjet+tjet).M(),weight)
									c8=1
								if bmass_cut:
									if c9==0:
										Mtb9.Fill((bjet+tjet).M(),weight)
										c9=1
									if btag_cut:
										if c10==0:
											Mtb10.Fill((bjet+tjet).M(),weight)


											HG_teta.Fill(tjet.Eta(),weight)   
											HG_beta.Fill(bjet.Eta(),weight)     
											HG_tpt.Fill(tjet.Perp(),weight)   
											HG_bpt.Fill(bjet.Perp(),weight)   
											HG_sumpt.Fill((bjet+tjet).Perp(),weight)    
											HG_tphi.Fill(tjet.Phi(),weight)      
											HG_bphi.Fill(bjet.Phi(),weight)     
											HG_dphi.Fill(abs(Math.VectorUtil.DeltaPhi(tjet,bjet)),weight)   
											c10=1	

    	if dysel:
		HG_drap.Fill(abs(tjet.Rapidity()-bjet.Rapidity()),weight)
		if (bjet+tjet).M()>1800.0:
			HG_drap_highmtb.Fill(abs(tjet.Rapidity()-bjet.Rapidity()),weight)
    	if tmasssel:
		HG_tmass.Fill(topJetMass[tindexval],weight)

        	event.getByLabel (AK8MassLabel, AK8MassHandle)
        	topJetMassAK8 	= 	AK8MassHandle.product()

        	event.getByLabel (prunedMassLabel, prunedMassHandle)
        	topJetMasspruned 	= 	prunedMassHandle.product()

        	event.getByLabel (filteredMassLabel, filteredMassHandle)
        	topJetMassfiltered 	= 	filteredMassHandle.product()

        	event.getByLabel (topMassLabel, topMassHandle)
        	topJetMasstopMass 	= 	topMassHandle.product()

        	event.getByLabel (trimmedMassLabel, trimmedMassHandle)
        	topJetMasstrimmed 	= 	trimmedMassHandle.product()

		HG_tmassAK8post.Fill(topJetMassAK8[tindexval],weight)
		HG_tmassprunedpost.Fill(topJetMasspruned[tindexval],weight)
		HG_tmassfilteredpost.Fill(topJetMassfiltered[tindexval],weight)
		HG_tmasstopMasspost.Fill(topJetMasstopMass[tindexval],weight)
		HG_tmasstrimmedpost.Fill(topJetMasstrimmed[tindexval],weight)
		HG_tmassjetmasspost.Fill(tjet.M(),weight)
		HG_tmasssoftdroppost.Fill(topJetMass[tindexval],weight)

    	if tmassselpre:
        	event.getByLabel (AK8MassLabel, AK8MassHandle)
        	topJetMassAK8 	= 	AK8MassHandle.product()

        	event.getByLabel (prunedMassLabel, prunedMassHandle)
        	topJetMasspruned 	= 	prunedMassHandle.product()

        	event.getByLabel (filteredMassLabel, filteredMassHandle)
        	topJetMassfiltered 	= 	filteredMassHandle.product()

        	event.getByLabel (topMassLabel, topMassHandle)
        	topJetMasstopMass 	= 	topMassHandle.product()

        	event.getByLabel (trimmedMassLabel, trimmedMassHandle)
        	topJetMasstrimmed 	= 	trimmedMassHandle.product()

		HG_tmassAK8.Fill(topJetMassAK8[tindexval],weight)
		HG_tmasspruned.Fill(topJetMasspruned[tindexval],weight)
		HG_tmassfiltered.Fill(topJetMassfiltered[tindexval],weight)
		HG_tmasstopMass.Fill(topJetMasstopMass[tindexval],weight)
		HG_tmasstrimmed.Fill(topJetMasstrimmed[tindexval],weight)
		HG_tmassjetmass.Fill(tjet.M(),weight)
		HG_tmasssoftdrop.Fill(topJetMass[tindexval],weight)

    	if nsjsel:
		HG_nsubjets.Fill(nSubjets[tindexval],weight)
    	if minmsel:
		HG_minmass.Fill(topJetminmass[tindexval],weight)
    	if sjbtagsel:
		HG_TopBDiscsjmaxCSV.Fill(SJ_csvmax,weight)
    	if t32sel:
		HG_tau32.Fill(tau3[tindexval]/tau2[tindexval],weight)
    	if bmasssel:
		HG_bmass.Fill(topJetMass[bindexval],weight)

        	event.getByLabel (AK8MassLabel, AK8MassHandle)
        	topJetMassAK8 	= 	AK8MassHandle.product()

        	event.getByLabel (prunedMassLabel, prunedMassHandle)
        	topJetMasspruned 	= 	prunedMassHandle.product()

        	event.getByLabel (filteredMassLabel, filteredMassHandle)
        	topJetMassfiltered 	= 	filteredMassHandle.product()

        	event.getByLabel (topMassLabel, topMassHandle)
        	topJetMasstopMass 	= 	topMassHandle.product()

        	event.getByLabel (trimmedMassLabel, trimmedMassHandle)
        	topJetMasstrimmed 	= 	trimmedMassHandle.product()

		HG_bmassAK8.Fill(topJetMassAK8[bindexval],weight)
		HG_bmasspruned.Fill(topJetMasspruned[bindexval],weight)
		HG_bmassfiltered.Fill(topJetMassfiltered[bindexval],weight)
		HG_bmasstopMass.Fill(topJetMasstopMass[bindexval],weight)
		HG_bmasstrimmed.Fill(topJetMasstrimmed[bindexval],weight)
		HG_bmassjetmass.Fill(bjet.M(),weight)
    	if btagsel:
		HG_csv.Fill(bJetBDisc[bindexval],weight)


f.cd()
f.Write()
f.Close()

print "number of events: " + str(count)


if options.printEvents:
    Outf1   =   open("DataEvents"+options.num+".txt", "w")
    sys.stdout = Outf1
    for goodEvent in goodEvents :
        print '{0:12.0f}:{1:12.0f}:{2:12.0f}'.format(
            goodEvent[0], goodEvent[1], goodEvent[2]
        )
