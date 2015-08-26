import os
import array
import glob
import math
import ROOT
import sys
from ROOT import *
from array import *
from optparse import OptionParser
parser = OptionParser()

parser.add_option('-c', '--cuts', metavar='F', type='string', action='store',
                  default	=	'default',
                  dest		=	'cuts',
                  help		=	'Cuts type (ie default, rate, etc)')
(options, args) = parser.parse_args()
cuts = options.cuts
gROOT.Macro("rootlogon.C")
import Wprime_Functions	
from Wprime_Functions import *

#titles=["PtScaling","TriggerWeighting","PtSmearing"]
#modsup=["ScaleUp","none","PtSmearUp"]
#modsdown=["ScaleDown","none","PtSmearDown"]
#trigsup=["nominal","up","nominal"]
#trigsdown=["nominal","down","nominal"]


LabelsU=['__jes__','__trig__','__ptsmear__']
mass = [1300,2000,2700]
rebin =4
for coup in ['right']:

	output = ROOT.TFile( "limitsetting/theta/Limits_allhadronic_"+coup+"_PSET_"+cuts+".root", "recreate" )
	output.cd()


	Data = ROOT.TFile("rootfiles/TBanalyzerQCD_PSET_"+options.cuts+"weighted.root")

	TTmc = ROOT.TFile("rootfiles/TBanalyzerttbar_PSET_"+options.cuts+"weighted.root")
	
	#TTmcScaleUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_ScaleUp_PSET_"+cuts+".root")
	#TTmcScaleDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_ScaleDown_PSET_"+cuts+".root")

	#TTmcQ2ScaleUp = ROOT.TFile("rootfiles/TBanalyzerttbarscaleup_Trigger_nominal_none_PSET_"+cuts+".root")
	#TTmcQ2ScaleDown = ROOT.TFile("rootfiles/TBanalyzerttbarscaledown_Trigger_nominal_none_PSET_"+cuts+".root")

	#TTmcPtSmearUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_PtSmearUp_PSET_"+cuts+".root")
	#TTmcPtSmearDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_PtSmearDown_PSET_"+cuts+".root")

	#TTmcEtaSmearUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_EtaSmearUp_PSET_"+cuts+".root")
	#TTmcEtaSmearDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_EtaSmearDown_PSET_"+cuts+".root")

	#TTmcTriggerUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_up_none_PSET_"+cuts+".root")
	#TTmcTriggerDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_down_none_PSET_"+cuts+".root")


	#STsmc = ROOT.TFile("rootfiles/TBanalyzerweightedsingletop_s_Trigger_nominal_none_PSET_"+cuts+".root")
	#STsBmc = ROOT.TFile("rootfiles/TBanalyzerweightedsingletop_sB_Trigger_nominal_none_PSET_"+cuts+".root")
	#STtmc = ROOT.TFile("rootfiles/TBanalyzerweightedsingletop_t_Trigger_nominal_none_PSET_"+cuts+".root")
	#STtBmc = ROOT.TFile("rootfiles/TBanalyzerweightedsingletop_tB_Trigger_nominal_none_PSET_"+cuts+".root")
	#STtWmc = ROOT.TFile("rootfiles/TBanalyzerweightedsingletop_tW_Trigger_nominal_none_PSET_"+cuts+".root")
	#STtWBmc = ROOT.TFile("rootfiles/TBanalyzerweightedsingletop_tWB_Trigger_nominal_none_PSET_"+cuts+".root")

	#starray = [
	#STsmc,
	#STsBmc, 
	#STtmc,
	#STtBmc,
	#STtWmc,
	#STtWBmc 
	#]

	#STsmcFS = STsmc.Get("Mtb")
	#STsBmcFS = STsBmc.Get("Mtb")
	#STtmcFS = STtmc.Get("Mtb")
	#STtBmcFS = STtBmc.Get("Mtb")
	#STtWmcFS = STtWmc.Get("Mtb")
	#STtWBmcFS = STtWBmc.Get("Mtb")

	#STsmcFS.Add(STsBmcFS)
	#STtmcFS.Add(STtBmcFS) 
	#STtWmcFS.Add(STtWBmcFS) 


	TTmcFS = TTmc.Get("Mtb")
	

	#TTmcQCD = TTmc.Get("QCDbkg")
	#TTmcQCDh = TTmc.Get("QCDbkgh")
	#TTmcQCDl = TTmc.Get("QCDbkgl")

	#TTmcFSScaleUp = TTmcScaleUp.Get("Mtb")
	#TTmcFSScaleDown = TTmcScaleDown.Get("Mtb")

	#TTmcFSQ2ScaleUp = TTmcQ2ScaleUp.Get("Mtb")
	#TTmcFSQ2ScaleDown = TTmcQ2ScaleDown.Get("Mtb")


	#TTmcFSTriggerUp = TTmcTriggerUp.Get("Mtb")
	#TTmcFSTriggerDown = TTmcTriggerDown.Get("Mtb")
	
	#TTmcFSPtSmearUp = TTmcPtSmearUp.Get("Mtb")
	#TTmcFSPtSmearDown = TTmcPtSmearDown.Get("Mtb")

	#TTmcFSEtaSmearUp = TTmcEtaSmearUp.Get("Mtb")
	#TTmcFSEtaSmearDown = TTmcEtaSmearDown.Get("Mtb")


	DataFS = Data.Get("Mtb")
	DataQCD = Data.Get("QCDbkg")
	DataQCD2d = Data.Get("QCDbkg2D")
	DataQCDUp = Data.Get("QCDbkgh")
	DataQCDDown = Data.Get("QCDbkgl")
	#TTmcQCD2d = TTmc.Get("QCDbkg2D")
	DataFS.Add(TTmc.Get("Mtb"))

	#DataQCD.Add(TTmcQCD,-1)
	#DataQCDUp.Add(TTmcQCD,-1)
	#DataQCDDown.Add(TTmcQCD,-1)
	#DataQCD2d.Add(TTmcQCD2d,-1)



	#for f in starray:
	#	DataQCD.Add(f.Get("QCDbkg"),-1)
	#	DataQCDUp.Add(f.Get("QCDbkg"),-1)
	#	DataQCDDown.Add(f.Get("QCDbkg"),-1)
	#	DataQCD2d.Add(f.Get("QCDbkg2D"),-1)


	fittitles = ["pol0","pol2","pol3","FIT","Bifpoly","expofit"]
	QCDbkg_ARR = []
	for ihist in range(0,len(fittitles)):
		QCDbkg_ARR.append(Data.Get("QCDbkg"+str(fittitles[ihist])).Rebin(rebin))

	BEfiterrh = Fit_Uncertainty(QCDbkg_ARR)


	DataQCD2d.Rebin(rebin)
	DataFS.Rebin(rebin)
	DataQCD.Rebin(rebin)
	DataQCDUp.Rebin(rebin)
	DataQCDDown.Rebin(rebin)
	#DataBEtptl.Rebin(rebin)
	#DataBEtpth.Rebin(rebin)
	TTmcFS.Rebin(rebin)

	#TTmcFSScaleUp.Rebin(rebin)
	#TTmcFSScaleDown.Rebin(rebin)
	#TTmcBup.Rebin(rebin)
	#TTmcBdown.Rebin(rebin)
	#TTmcFSPtSmearUp.Rebin(rebin)
	#TTmcFSPtSmearDown.Rebin(rebin)
	#TTmcFSTriggerUp.Rebin(rebin)
	#TTmcFSTriggerDown.Rebin(rebin)
	#TTmcptup.Rebin(rebin)
	#TTmcptdown.Rebin(rebin)


	#TTmcFSQ2ScaleUp.Rebin(rebin)
	#TTmcFSQ2ScaleDown.Rebin(rebin)

	#STsmcFS.Rebin(rebin)
	#STtmcFS.Rebin(rebin)
	#STtWmcFS.Rebin(rebin)

	DataQCDE1Up = DataQCD.Clone()	
	DataQCDE2Up = DataQCD.Clone()	
	DataQCDE1Down = DataQCD.Clone()	
	DataQCDE2Down = DataQCD.Clone()
	for ibin in range(0,DataQCD.GetNbinsX()+1):
		QCDfit3=abs(DataQCD2d.GetBinContent(ibin)-DataQCD.GetBinContent(ibin))
		QCDfit2=abs(BEfiterrh.GetBinContent(ibin))
		DataQCDE1Up.SetBinContent(ibin,DataQCD.GetBinContent(ibin)+QCDfit3)
		DataQCDE1Down.SetBinContent(ibin,max(0.0,DataQCD.GetBinContent(ibin)-QCDfit3))
		DataQCDE2Up.SetBinContent(ibin,DataQCD.GetBinContent(ibin)+QCDfit2)
		DataQCDE2Down.SetBinContent(ibin,max(0.0,DataQCD.GetBinContent(ibin)-QCDfit2))
	



	DataFS.SetName("mtb_allhad__DATA")
	DataQCD.SetName("mtb_allhad__qcd")
	DataQCDUp.SetName("mtb_allhad__qcd__Fit__up")
	DataQCDDown.SetName("mtb_allhad__qcd__Fit__down")
	DataQCDE1Up.SetName("mtb_allhad__qcd__TwoD__up")
	DataQCDE1Down.SetName("mtb_allhad__qcd__TwoD__down")
	DataQCDE2Up.SetName("mtb_allhad__qcd__Alt__up")
	DataQCDE2Down.SetName("mtb_allhad__qcd__Alt__down")

	TTmcFS.SetName("mtb_allhad__ttbar")
	#TTmcFSScaleUp.SetName("mtb_allhad__ttbar__jes__up")
	#TTmcFSScaleDown.SetName("mtb_allhad__ttbar__jes__down")

	#TTmcFSQ2ScaleUp.SetName("mtb_allhad__ttbar__q2__up")
	#TTmcFSQ2ScaleDown.SetName("mtb_allhad__ttbar__q2__down")

	#TTmcBup.SetName("mtb_allhad__ttbar__btag__up")
	#TTmcBdown.SetName("mtb_allhad__ttbar__btag__down")

	#TTmcptup.SetName("mtb_allhad__ttbar__pt__up")
	#TTmcptdown.SetName("mtb_allhad__ttbar__pt__down")

	#TTmcFSPtSmearUp.SetName("mtb_allhad__ttbar__ptsmear__up")
	#TTmcFSPtSmearDown.SetName("mtb_allhad__ttbar__ptsmear__down")

	#TTmcFSTriggerUp.SetName("mtb_allhad__ttbar__trig__up")
	#TTmcFSTriggerDown.SetName("mtb_allhad__ttbar__trig__down")

	#STsmcFS.SetName("mtb_allhad__sts")
	#STtmcFS.SetName("mtb_allhad__stt")
	#STtWmcFS.SetName("mtb_allhad__sttw")

	DataFS.SetTitle("mtb_allhad__DATA")
	DataQCD.SetTitle("mtb_allhad__qcd")
	DataQCDUp.SetTitle("mtb_allhad__qcd__Fit__up")
	DataQCDDown.SetTitle("mtb_allhad__qcd__Fit__down")
	DataQCDE1Up.SetTitle("mtb_allhad__qcd__TwoD__up")
	DataQCDE1Down.SetTitle("mtb_allhad__qcd__TwoD__down")
	DataQCDE2Up.SetTitle("mtb_allhad__qcd__Alt__up")
	DataQCDE2Down.SetTitle("mtb_allhad__qcd__Alt__down")

	TTmcFS.SetTitle("mtb_allhad__ttbar")
	#TTmcFSScaleUp.SetTitle("mtb_allhad__ttbar__jes__up")
	#TTmcFSScaleDown.SetTitle("mtb_allhad__ttbar__jes__down")
	
	#TTmcFSQ2ScaleUp.SetTitle("mtb_allhad__ttbar__q2__up")
	#TTmcFSQ2ScaleDown.SetTitle("mtb_allhad__ttbar__q2__down")

	#TTmcBup.SetTitle("mtb_allhad__ttbar__btag__up")
	#TTmcBdown.SetTitle("mtb_allhad__ttbar__btag__down")

	#TTmcptup.SetTitle("mtb_allhad__ttbar__pt__up")
	#TTmcptdown.SetTitle("mtb_allhad__ttbar__pt__down")

	#TTmcFSPtSmearUp.SetTitle("mtb_allhad__ttbar__ptsmear__up")
	#TTmcFSPtSmearDown.SetTitle("mtb_allhad__ttbar__ptsmear__down")

	#TTmcFSTriggerUp.SetTitle("mtb_allhad__ttbar__trig__up")
	#TTmcFSTriggerDown.SetTitle("mtb_allhad__ttbar__trig__down")

	#STsmcFS.SetTitle("mtb_allhad__sts")
	#STtmcFS.SetTitle("mtb_allhad__stt")
	#STtWmcFS.SetTitle("mtb_allhad__sttw")

        output.cd()

	DataFS.Write("mtb_allhad__DATA")
	DataQCD.Write("mtb_allhad__qcd")
	DataQCDUp.Write("mtb_allhad__qcd__Fit__up")
	DataQCDDown.Write("mtb_allhad__qcd__Fit__down")
	DataQCDE1Up.Write("mtb_allhad__qcd__TwoD__up")
	DataQCDE1Down.Write("mtb_allhad__qcd__TwoD__down")
	DataQCDE2Up.Write("mtb_allhad__qcd__Alt__up")
	DataQCDE2Down.Write("mtb_allhad__qcd__Alt__down")
	#DataQCDBEH.Write("mtb_allhad__qcd__bkg__up")
	#DataQCDBEL.Write("mtb_allhad__qcd__bkg__down")

	TTmcFS.Write("mtb_allhad__ttbar")
	#TTmcFSScaleUp.Write("mtb_allhad__ttbar__jes__up")
	#TTmcFSScaleDown.Write("mtb_allhad__ttbar__jes__down")

	#TTmcFSQ2ScaleUp.Write("mtb_allhad__ttbar__q2__up")
	#TTmcFSQ2ScaleDown.Write("mtb_allhad__ttbar__q2__down")

	#TTmcBup.Write("mtb_allhad__ttbar__btag__up")
	#TTmcBdown.Write("mtb_allhad__ttbar__btag__down")

	#TTmcptup.Write("mtb_allhad__ttbar__pt__up")
	#TTmcptdown.Write("mtb_allhad__ttbar__pt__down")

	#TTmcFSPtSmearUp.Write("mtb_allhad__ttbar__ptsmear__up")
	#TTmcFSPtSmearDown.Write("mtb_allhad__ttbar__ptsmear__down")

	#TTmcFSTriggerUp.Write("mtb_allhad__ttbar__trig__up")
	#TTmcFSTriggerDown.Write("mtb_allhad__ttbar__trig__down")

	#STsmcFS.Write("mtb_allhad__sts")
	#STtmcFS.Write("mtb_allhad__stt")
	#STtWmcFS.Write("mtb_allhad__sttw")

	for RA in range(0,len(mass)):
		coup = "right"
		SignalB11 = ROOT.TFile("rootfiles/TBanalyzerweightedsignal"+coup+str(mass[RA])+"_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+cuts+".root")


		SignalFS = SignalB11.Get("Mtb")
		#SignalBup = SignalB11.Get("MtbBup")
		#SignalBdown = SignalB11.Get("MtbBDown")

		output.cd()

		SignalFS.Rebin(rebin)
		#SignalBup.Rebin(rebin)
		#SignalBdown.Rebin(rebin)

		SignalFS.SetTitle("mtb_allhad__wp"+str(mass[RA]))
		#SignalBup.SetTitle("mtb_allhad__wp"+str(mass[RA])+"__btag__up")
		#SignalBdown.SetTitle("mtb_allhad__wp"+str(mass[RA])+"__btag__down")

		SignalFS.SetName("mtb_allhad__wp"+str(mass[RA]))
		#SignalBup.SetName("mtb_allhad__wp"+str(mass[RA])+"__btag__up")
		#SignalBdown.SetName("mtb_allhad__wp"+str(mass[RA])+"__btag__down")

		SignalFS.Write("mtb_allhad__wp"+str(mass[RA]))
		#SignalBup.Write("mtb_allhad__wp"+str(mass[RA])+"__btag__up")
		#SignalBdown.Write("mtb_allhad__wp"+str(mass[RA])+"__btag__down")






	
