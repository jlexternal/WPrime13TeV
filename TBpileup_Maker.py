
import os
import array
import glob
import math
import ROOT
import sys
from array import *
from ROOT import *

leg = TLegend(0.5, 0.5, 0.84, 0.84)
leg.SetFillColor(0)
leg.SetBorderSize(0)

ROOT.gROOT.Macro("rootlogon.C")
fdata = ROOT.TFile("Pileup64900.root")
fdataup = ROOT.TFile("Pileup72900.root")
fdataalt = ROOT.TFile("MyDataPileupHistogram.root")
fttbar = ROOT.TFile("ttbarPU.root")


output = ROOT.TFile( "PileUp_Ratio_ttbar.root", "recreate" )

output.cd()

# Get numerators and denominators for each eta region

ndata = fdata.Get("pileup")
ndataup = fdataup.Get("pileup")

ndata.Sumw2()
ndataup.Sumw2()

ndata.Scale(1./ndata.Integral())
ndataup.Scale(1./ndataup.Integral())

ndataalt = fdataalt.Get("pileup")

ndataalt.Sumw2()

ndataalt.Scale(1./ndataalt.Integral())

dttbar = fttbar.Get("npvrealtrue")
dttbar.Scale(1./dttbar.Integral())

dttbaralt = fttbar.Get("npvrealtruealt")
dttbaralt.Scale(1./dttbaralt.Integral())

ttbar_pileup_reweight = ndata.Clone("Pileup_Ratio")
ttbar_pileup_reweight.Divide(dttbar)

ttbar_pileup_reweightalt = ndataalt.Clone("ttbar_pileup_reweight_alt")
ttbar_pileup_reweightalt.Divide(dttbaralt)

ttbar_pileup_reweight.Write()
ttbar_pileup_reweight_up = ndataup.Clone("ttbar_pileup_reweight_up")
ttbar_pileup_reweight_up.Divide(dttbar)
ttbar_pileup_reweight_up.Write()

files = [
ROOT.TFile("signal_1300PU.root"),
ROOT.TFile("signal_1500PU.root"),
ROOT.TFile("signal_1700PU.root"),
ROOT.TFile("signal_1900PU.root"),
ROOT.TFile("signal_2100PU.root"),
ROOT.TFile("signal_2300PU.root"),
ROOT.TFile("signal_2700PU.root"),
ROOT.TFile("signal_3100PU.root")
]

names = [
"PileUp_Ratio_signal1300.root",
"PileUp_Ratio_signal1500.root",
"PileUp_Ratio_signal1700.root",
"PileUp_Ratio_signal1900.root",
"PileUp_Ratio_signal2100.root",
"PileUp_Ratio_signal2300.root",
"PileUp_Ratio_signal2700.root",
"PileUp_Ratio_signal3100.root"
]

dhists = []
dhistsalt = []
for ifile in range(0,len(files)):
	outputsig = ROOT.TFile(names[ifile] , "recreate" )
	outputsig.cd()



	dhists.append(files[ifile].Get("npvrealtrue"))
	dhists[ifile].Scale(1./dhists[ifile].Integral())

	dhistsalt.append(files[ifile].Get("npvrealtruealt"))
	dhistsalt[ifile].Scale(1./dhistsalt[ifile].Integral())

	Pileup_Ratio = ndata.Clone("Pileup_Ratio")



	Pileup_Ratio_up = ndataup.Clone("Pileup_Ratio_up")
	Pileup_Ratio.Divide(dhists[ifile])

	Pileup_Ratio_up.Divide(dhists[ifile])
	Pileup_Ratio.Write()
	Pileup_Ratio_up.Write()
	#outputsig.Write()
	outputsig.Close()

	outputsigalt = ROOT.TFile("alt"+names[ifile] , "recreate" )
	outputsigalt.cd()
	Pileup_Ratioalt = ndataalt.Clone("Pileup_Ratioalt")
	Pileup_Ratioalt.Divide(dhistsalt[ifile])
	Pileup_Ratioalt.Write()
	outputsigalt.Close()
