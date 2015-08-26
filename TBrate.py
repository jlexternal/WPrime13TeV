#! /usr/bin/env python

###################################################################
##								 ##
## Name: TBrate.py						 ##
## Author: Kevin Nash 						 ##
## Date: 6/5/2012						 ##
## Purpose: This program creates eta binned tags and probes 	 ##
##          as a function of Pt for data and MC for use with 	 ##
##          TBrate_Maker.py.					 ##
##								 ##
###################################################################

import os
import glob
import math
from math import sqrt,exp
import ROOT
from ROOT import std,ROOT,TFile,TLorentzVector,TMath,gROOT, TF1,TH1F,TH1D,TH2F,TH2D
from ROOT import TVector
from ROOT import TFormula

import sys
from DataFormats.FWLite import Events, Handle
from optparse import OptionParser
from array import *

parser = OptionParser()

parser.add_option('-s', '--set', metavar='F', type='string', action='store',
                  default	=	'data',
                  dest		=	'set',
                  help		=	'dataset (ie data,ttbar etc)')
parser.add_option('-u', '--ptreweight', metavar='F', type='string', action='store',
                  default	=	'none',
                  dest		=	'ptreweight',
                  help		=	'on or off')
parser.add_option('-t', '--tname', metavar='F', type='string', action='store',
                  default	=	'HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1',
                  dest		=	'tname',
                  help		=	'trigger name')

parser.add_option('-n', '--num', metavar='F', type='string', action='store',
                  default	=	'all',
                  dest		=	'num',
                  help		=	'job number')
parser.add_option('-j', '--jobs', metavar='F', type='string', action='store',
                  default	=	'1',
                  dest		=	'jobs',
                  help		=	'number of jobs')
parser.add_option('-g', '--grid', metavar='F', type='string', action='store',
                  default	=	'off',
                  dest		=	'grid',
                  help		=	'running on grid off or on')
parser.add_option('-c', '--cuts', metavar='F', type='string', action='store',
                  default	=	'rate_default',
                  dest		=	'cuts',
                  help		=	'Cuts type (ie default, rate, etc)')
parser.add_option('-b', '--bx', metavar='F', type='string', action='store',
                  default	=	'50ns',
                  dest		=	'bx',
                  help		=	'bunch crossing 50ns or 25ns')



(options, args) = parser.parse_args()



tname = options.tname.split(',')
tnamestr = ''
for iname in range(0,len(tname)):
	tnamestr+=tname[iname]
	if iname!=len(tname)-1:
		tnamestr+='OR'
	

print "Options summary"
print "=================="
for  opt,value in options.__dict__.items():
	#print str(option)+ ": " + str(options[option]) 
	print str(opt) +': '+ str(value)
print "=================="
print ""

#If running on the grid we access the script within a tarred directory
di = ""
if options.grid == 'on':
	di = "tardir/"
	sys.path.insert(0, 'tardir/')

gROOT.Macro(di+"rootlogon.C")
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

	#TrigFile = TFile(di+"Triggerweight_"+options.set+".root")
	TrigFile = TFile(di+"Triggerweight_signalright2000btags.root")
	TrigPlot = TrigFile.Get("TriggerWeight_"+tnamestr)

	#PileFile = TFile(di+"PileUp_Ratio_"+settype+".root")
	#PilePlot = PileFile.Get("Pileup_Ratio")




# We select all the events:    
events = Events (files)

#Here we load up handles and labels.
#These are used to grab entries from the Ntuples.
#To see all the current types in an Ntuple use edmDumpEventContent /PathtoNtuple/Ntuple.root

AK8HL = Initlv("jetsAK8")
	

GeneratorHandle 	= 	Handle (  "GenEventInfoProduct")
GeneratorLabel  	= 	( "generator" , "")



BDiscHandle 	= 	Handle (  "vector<float> "  )
BDiscLabel  	= 	( "jetsAK8" , "jetAK8CSV")


puHandle    	= 	Handle("int")
puLabel     	= 	( "eventUserData", "puNtrueInt" )

minmassHandle 	= 	Handle (  "vector<float> "  )
minmassLabel  	= 	( "jetsAK8" , "jetAK8minmass")

nSubjetsHandle 	= 	Handle (  "vector<float> "  )
nSubjetsLabel  	= 	( "jetsAK8" , "jetAK8nSubJets")


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

#jetKeysAK8Handle 	= 	Handle (  "vector<vector<int> >"  )
#jetKeysAK8Label  	= 	( "jetKeysAK8" , "")

#subjetKeysAK8Handle 	= 	Handle (  "vector<vector<int> >"  )
#subjetKeysAK8Label  	= 	( "subjetKeysAK8" , "")

#---------------------------------------------------------------------------------------------------------------------#

#Create the output file
if jobs != 1:
	f = TFile( "TBratefile"+options.set+"_job"+options.num+"of"+options.jobs+"_PSET_"+options.cuts+".root", "recreate" )
else:
	f = TFile( "TBratefile"+options.set+"_PSET_"+options.cuts+".root", "recreate" )




print "Creating histograms"

#Define Histograms
f.cd()
#---------------------------------------------------------------------------------------------------------------------#
Nevents	    = TH1F("Nevents",     	  "mass of tb",     	  	         5, 0., 5. )

pteta1pretag	= TH1D("pteta1pretag",           "b Probe pt in 0<Eta<0.6",             400,  0,  2000 )
pteta2pretag	= TH1D("pteta2pretag",           "b Probe pt in 0.6<Eta<1.25",          400,  0,  2000 )
pteta3pretag	= TH1D("pteta3pretag",           "b Probe pt in 1.25<Eta<2.5",          400,  0,  2000 )

pteta1          = TH1D("pteta1",           "b pt in 0<Eta<0.6",             	400,  0,  2000 )
pteta2          = TH1D("pteta2",           "b pt in 0.6<Eta<1.25",             	400,  0,  2000 )
pteta3          = TH1D("pteta3",           "b pt in 1.25<Eta<2.5",             	400,  0,  2000 )

pteta1pretag.Sumw2()
pteta2pretag.Sumw2()
pteta3pretag.Sumw2()

pteta1.Sumw2()
pteta2.Sumw2()
pteta3.Sumw2()

MtbbptcomparepreSB1e1    = TH2F("MtbbptcomparepreSB1e1",  "Comparison bpt and Mtb",   		400,0,2000,  140,  500,  4000 )
MtbbptcomparepostSB1e1    = TH2F("MtbbptcomparepostSB1e1",  "Comparison bpt and Mtb",   		400,0,2000,  140,  500,  4000 )

MtbbptcomparepreSB1e1.Sumw2()
MtbbptcomparepostSB1e1.Sumw2()

MtbbptcomparepreSB1e2    = TH2F("MtbbptcomparepreSB1e2",  "Comparison bpt and Mtb",   		400,0,2000,  140,  500,  4000 )
MtbbptcomparepostSB1e2    = TH2F("MtbbptcomparepostSB1e2",  "Comparison bpt and Mtb",   		400,0,2000,  140,  500,  4000 )

MtbbptcomparepreSB1e2.Sumw2()
MtbbptcomparepostSB1e2.Sumw2()

MtbbptcomparepreSB1e3    = TH2F("MtbbptcomparepreSB1e3",  "Comparison bpt and Mtb",   		400,0,2000,  140,  500,  4000 )
MtbbptcomparepostSB1e3    = TH2F("MtbbptcomparepostSB1e3",  "Comparison bpt and Mtb",   		400,0,2000,  140,  500,  4000 )

MtbbptcomparepreSB1e3.Sumw2()
MtbbptcomparepostSB1e3.Sumw2()



#---------------------------------------------------------------------------------------------------------------------#

# loop over events
#---------------------------------------------------------------------------------------------------------------------#

count = 0
jobiter = 0
print "Start looping"
#initialize the ttree variables
tree_vars = {"bpt":array('d',[0.]),"bmass":array('d',[0.]),"btag":array('d',[0.]),"tpt":array('d',[0.]),"tmass":array('d',[0.]),"nsubjets":array('d',[0.]),"sjbtag":array('d',[0.])}
Tree = Make_Trees(tree_vars)
totevents = events.size()
print str(totevents)  +  ' Events total'

usegenweight = False
if options.set == "QCDFLAT7000":
	usegenweight = True
	print "Using gen weight"

for event in events:
    count	= 	count + 1
    weightSFb = 1.0
    errorSFb = 0.0

    #Uncomment for a low count test run
    #if count > 300000:
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
    #We consider both the case that the b is the leading (highest pt) jet (hemis0) and the case where the top is the leading jet (hemis1)
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
	#Cuts are loaded from the Wprime_Functions.py file
	#here bpt[0] is 370 and bpt[1] is inf, so we are making sure the b pt is at least 370 GeV
    	bpt_cut = bpt[0]<bjet.Perp()<bpt[1]
    	tpt_cut = tpt[0]<tjet.Perp()<tpt[1]
    	dy_cut = dy[0]<=abs(tjet.Rapidity()-bjet.Rapidity())<dy[1]
    	#We first perform the top and b candidate pt cuts and the deltaY cut

	if usegenweight:
		try:
			weight*=gen.weightProduct()
		except:
			continue 

    	if bpt_cut and tpt_cut and dy_cut: 

		if False:# options.set!="data":
			#Pileup reweighting is done here 
			event.getByLabel (puLabel, puHandle)
    			PileUp 		= 	puHandle.product()
                	bin1 = PilePlot.FindBin(PileUp[0]) 
			weight *= PilePlot.GetBinContent(bin1)
			if run_b_SF :
				#btagging scale factor reweighting done here
				SFB = SFB_Lookup( bjet.Perp() )
				weightSFb = SFB[0]
				errorSFb = SFB[1]

        	event.getByLabel (softDropMassLabel, softDropMassHandle)
        	topJetMass 	= 	softDropMassHandle.product()
        	event.getByLabel ( nSubjetsLabel , nSubjetsHandle )
    		nSubjets 		= 	nSubjetsHandle.product()
        	event.getByLabel (minmassLabel, minmassHandle)
    		topJetminmass 	= 	minmassHandle.product()
		tmass_cut = tmass[0]<topJetMass[tindexval]<tmass[1]
		nsubjets_cut = nsubjets[0]<=nSubjets[tindexval]<nsubjets[1]

		#Now we start top-tagging.  In this file, we use a sideband based on inverting some top-tagging requirements
        	if tmass_cut and nsubjets_cut :
                	event.getByLabel (BDiscLabel, BDiscHandle)
                	bJetBDisc 	= 	BDiscHandle.product()
                	btag_cut = btag[0]<bJetBDisc[bindexval]<=btag[1]

			ht = tjet.Perp() + bjet.Perp()
			if tname != [] and options.set!='data' :
				
				#Trigger reweighting done here
				TRW = Trigger_Lookup( ht , TrigPlot )
				weight*=TRW



			if False:#options.ptreweight == "on":
				#ttbar pt reweighting done here
				event.getByLabel( GenLabel, GenHandle )
				GenParticles = GenHandle.product()
				PTW = PTW_Lookup( GenParticles )
				weight*=PTW

			weightb=weight*weightSFb
			weightSFbup=weight*(weightSFb+errorSFb)
			weightSFbdown=weight*(weightSFb-errorSFb)



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



			SJ_csvs = [subjets0index,subjets1index,subjets2index,subjets3index]
			
			SJ_csvvals = []
			for icsv in range(0,int(nSubjets[tindexval])):
				if int(SJ_csvs[icsv][tindexval])!=-1:
					SJ_csvvals.append(subjetsCSV[int(SJ_csvs[icsv][tindexval])])
				else:
					SJ_csvvals.append(0.)
			SJ_csvmax = max(SJ_csvvals)
			sjbtag_cut = sjbtag[0]<SJ_csvmax<=sjbtag[1]
			if sjbtag_cut:
				bmass_cut = bmass[0]<=topJetMass[bindexval]<bmass[1]

				if bmass_cut:

					eta1_cut = eta1[0]<=abs(bjet.Eta())<eta1[1]
					eta2_cut = eta2[0]<=abs(bjet.Eta())<eta2[1]
					eta3_cut = eta3[0]<=abs(bjet.Eta())<eta3[1]
					#Extract tags and probes for the average b tagging rate here 
					#We use three eta regions 
					if eta1_cut:		
						MtbbptcomparepreSB1e1.Fill(bjet.Perp(),(tjet+bjet).M(),weight)
                				pteta1pretag.Fill( bjet.Perp(),weight)
                				if btag_cut :
							MtbbptcomparepostSB1e1.Fill(bjet.Perp(),(tjet+bjet).M(),weightb)
                					pteta1.Fill( bjet.Perp(),weightb)
					if eta2_cut:
						MtbbptcomparepreSB1e2.Fill(bjet.Perp(),(tjet+bjet).M(),weight)
                				pteta2pretag.Fill( bjet.Perp(),weight)
                				if btag_cut :
							MtbbptcomparepostSB1e2.Fill(bjet.Perp(),(tjet+bjet).M(),weightb)
                					pteta2.Fill( bjet.Perp(),weightb)
					if eta3_cut:
						MtbbptcomparepreSB1e3.Fill(bjet.Perp(),(tjet+bjet).M(),weight)
                				pteta3pretag.Fill( bjet.Perp(),weight)
                				if btag_cut :
							MtbbptcomparepostSB1e3.Fill(bjet.Perp(),(tjet+bjet).M(),weightb)
                					pteta3.Fill( bjet.Perp(),weightb)
					temp_variables = {"bpt":bjet.Perp(),"bmass":topJetMass[bindexval],"btag":bJetBDisc[bindexval],"tpt":tjet.Perp(),"tmass":topJetMass[tindexval],"nsubjets":nSubjets[tindexval],"sjbtag":SJ_csvmax}

					for tv in tree_vars.keys():
						tree_vars[tv][0] = temp_variables[tv]
					Tree.Fill()


f.cd()
f.Write()
f.Close()

print "number of events: " + str(count)
