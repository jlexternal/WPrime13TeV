

import os
import array
import glob
import math
import ROOT
import sys
from ROOT import *
from array import *
from optparse import OptionParser
gROOT.Macro("rootlogon.C")
gROOT.LoadMacro("insertlogo.C+")
parser = OptionParser()

parser.add_option('-c', '--cuts', metavar='F', type='string', action='store',
                  default	=	'default',
                  dest		=	'cuts',
                  help		=	'Cuts type (ie default, rate, etc)')
(options, args) = parser.parse_args()

cuts = options.cuts


import Wprime_Functions	
from Wprime_Functions import *

st1= ROOT.THStack("st1", "st1")

leg = TLegend(0.45, 0.35, 0.84, 0.84)
leg.SetFillColor(0)
leg.SetBorderSize(0)

leg1 = TLegend(0.45, 0.5, 0.84, 0.84)
leg1.SetFillColor(0)
leg1.SetBorderSize(0)


leg2 = TLegend(0.5, 0.5, 0.84, 0.84)
leg2.SetFillColor(0)
leg2.SetBorderSize(0)

rebin =4

Mult = 1.0

#Kfac=1.2



sigf = [
ROOT.TFile("rootfiles/TBkinematicsweightedsignalright1300_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+options.cuts+".root"),
ROOT.TFile("rootfiles/TBkinematicsweightedsignalright2000_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+options.cuts+".root"),
ROOT.TFile("rootfiles/TBkinematicsweightedsignalright2700_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+options.cuts+".root")
]

sigh = [
sigf[0].Get("Mtb9"),
sigf[1].Get("Mtb9"),
sigf[2].Get("Mtb9")
]

sigh[0].Scale(Mult)
sigh[1].Scale(Mult)
sigh[2].Scale(Mult)

sigh[0].Rebin(rebin)
sigh[1].Rebin(rebin)
sigh[2].Rebin(rebin)

sigh[0].SetLineStyle(5)
sigh[1].SetLineStyle(6)
sigh[2].SetLineStyle(7)

sigh[0].SetLineWidth(2)
sigh[1].SetLineWidth(2)
sigh[2].SetLineWidth(2)

sigh[0].SetLineColor(1)
sigh[1].SetLineColor(2)
sigh[2].SetLineColor(4)

stops = ['singletop_s','singletop_sB','singletop_t','singletop_tB','singletop_tW','singletop_tWB']

sfiles=[]
shists=[]
ssubs=[]
ssubsh=[]
ssubsl=[]


DataB11 = ROOT.TFile("rootfiles/TBkinematicsQCD_PSET_"+options.cuts+"weighted.root")
TTmc 	= ROOT.TFile("rootfiles/TBkinematicsttbar_PSET_"+options.cuts+"weighted.root")

#DataFS 	= DataB11.Get("Mtb") 			# QCD FS
DataFS 	= DataB11.Get("bmass") 			# QCD FS
DataBE 	= DataB11.Get("QCDbkg") 		# QCDbkg

DataBE2d = DataB11.Get("QCDbkg2D") 

c1 = TCanvas('c1', 'QCD Full selection vs b pt tagging background', 700, 600)

main = ROOT.TPad("main", "main", 0, 0.3, 1, 1)
sub = ROOT.TPad("sub", "sub", 0, 0, 1, 0.3)

main.SetLeftMargin(0.16)
main.SetRightMargin(0.05)
main.SetTopMargin(0.11)
main.SetBottomMargin(0.0)

sub.SetLeftMargin(0.16)
sub.SetRightMargin(0.05)
sub.SetTopMargin(0)
sub.SetBottomMargin(0.3)

main.Draw()
sub.Draw()

main.cd()




#TTmcScaleUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_ScaleUp_PSET_"+options.cuts+".root")
#TTmcScaleDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_ScaleDown_PSET_"+options.cuts+".root")

#TTmcPtSmearUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_PtSmearUp_PSET_"+options.cuts+".root")
#TTmcPtSmearDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_PtSmearDown_PSET_"+options.cuts+".root")

#TTmcQ2ScaleUp = ROOT.TFile("rootfiles/TBanalyzerttbarscaleup_Trigger_nominal_none_PSET_"+options.cuts+".root")
#TTmcQ2ScaleDown = ROOT.TFile("rootfiles/TBanalyzerttbarscaledown_Trigger_nominal_none_PSET_"+options.cuts+".root")




#TTmcEtaSmearUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_EtaSmearUp_PSET_"+options.cuts+".root")
#TTmcEtaSmearDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_nominal_EtaSmearDown_PSET_"+options.cuts+".root")

#TTmcTriggerUp = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_up_none_PSET_"+options.cuts+".root")
#TTmcTriggerDown = ROOT.TFile("rootfiles/TBanalyzerttbar_Trigger_down_none_PSET_"+options.cuts+".root")

output = ROOT.TFile( "TBkinematics_output_PSET_"+options.cuts+".root", "recreate" )
output.cd()

TTmcFS = TTmc.Get("Mtb9")

TTmcBE = TTmc.Get("QCDbkg")

TTmcBE2d = TTmc.Get("QCDbkg2D")


TTmcBEh = TTmc.Get("QCDbkgh")
DataBEh = DataB11.Get("QCDbkgh")

TTmcBEl = TTmc.Get("QCDbkgl")
DataBEl = DataB11.Get("QCDbkgl")

#TTmcFSScaleUp = TTmcScaleUp.Get("Mtb")
#TTmcFSScaleDown = TTmcScaleDown.Get("Mtb")

#TTmcFSQ2ScaleUp = TTmcQ2ScaleUp.Get("Mtb")
#TTmcFSQ2ScaleDown = TTmcQ2ScaleDown.Get("Mtb")

#TTmcFSPtSmearUp = TTmcPtSmearUp.Get("Mtb")
#TTmcFSPtSmearDown = TTmcPtSmearDown.Get("Mtb")

#TTmcFSEtaSmearUp = TTmcEtaSmearUp.Get("Mtb")
#TTmcFSEtaSmearDown = TTmcEtaSmearDown.Get("Mtb")

#TTmcFSTriggerUp = TTmcTriggerUp.Get("Mtb")
#TTmcFSTriggerDown = TTmcTriggerDown.Get("Mtb")

TTmcFSBUp =  TTmc.Get("MtbBup")
TTmcFSBDown =  TTmc.Get("MtbBDown")

TTmcFSptup =  TTmc.Get("Mtbptup")
TTmcFSptdown =  TTmc.Get("Mtbptdown")

TTmcFSBUp.Rebin(rebin)
TTmcFSBDown.Rebin(rebin)

TTmcFSptup.Rebin(rebin)
TTmcFSptdown.Rebin(rebin)

#TTmcFSQ2ScaleUp.Rebin(rebin)
#TTmcFSQ2ScaleDown.Rebin(rebin)

#TTmcFSScaleUp.Rebin(rebin)
#TTmcFSScaleDown.Rebin(rebin)

#TTmcFSPtSmearUp.Rebin(rebin)
#TTmcFSPtSmearDown.Rebin(rebin)

#TTmcFSEtaSmearUp.Rebin(rebin)
#TTmcFSEtaSmearDown.Rebin(rebin)

#TTmcFSTriggerUp.Rebin(rebin)
#TTmcFSTriggerDown.Rebin(rebin)


DataBE2d.Rebin(rebin)
TTmcFS.Rebin(rebin)
DataBE.Rebin(rebin)
DataFS.Rebin(rebin)
print DataFS.Integral()
DataBEl.Rebin(rebin)
DataBEh.Rebin(rebin)
TTmcBE.Rebin(rebin)
TTmcBE2d.Rebin(rebin)
TTmcBEh.Rebin(rebin)
TTmcBEl.Rebin(rebin)

#subtract weighted TT pretags

unsubbkg = DataBE.Clone()

# Uncomment when using data -JL #

#DataBE.Add(TTmcBE,-1)
#DataBE2d.Add(TTmcBE2d,-1)
#DataBEl.Add(TTmcBE,-1)
#DataBEh.Add(TTmcBE,-1)

singletop = ROOT.TH1F("singletop",     "singletop",     	  	      140, 500, 4000 )
singletop.Rebin(rebin)

schanst = ROOT.TH1F("schanst",     "singletop",     	  	      140, 500, 4000 )
schanst.Rebin(rebin)

###take out and remove if when taking out
if False:
	for ifile in range(0,len(stops)):
		sfiles.append(ROOT.TFile("rootfiles/TBkinematicsweighted"+stops[ifile]+"_Trigger_nominal_none_PSET_"+options.cuts+".root"))
		shists.append(sfiles[ifile].Get("Mtb"))
		ssubs.append(sfiles[ifile].Get("QCDbkg"))

		ssubsh.append(sfiles[ifile].Get("QCDbkgh"))
		ssubsl.append(sfiles[ifile].Get("QCDbkgl"))
		shists[ifile].Rebin(rebin)
		ssubs[ifile].Rebin(rebin)
		ssubsh[ifile].Rebin(rebin)
		ssubsl[ifile].Rebin(rebin)
		#print str((Luminosity*stopxsecs[ifile]*TeffScale)/stopnevents[ifile])    
		DataBE.Add(ssubs[ifile],-1)
		DataBEl.Add(ssubsl[ifile],-1)
		DataBEh.Add(ssubsh[ifile],-1)  
		singletop.SetFillColor(6)
		singletop.Add(shists[ifile])
		if ifile<=1:
			schanst.Add(shists[ifile])

st1.Add(singletop)

output.cd()

fittitles = ["pol0","pol2","pol3","FIT","Bifpoly","expofit"]
QCDbkg_ARR = []
for ihist in range(0,len(fittitles)):
	QCDbkg_ARR.append(DataB11.Get("QCDbkg"+str(fittitles[ihist])).Rebin(rebin))

BEfiterrh = Fit_Uncertainty(QCDbkg_ARR)

output.cd()
DataQCDBEH=DataBE.Clone("DataQCDBEH")
DataQCDBEL=DataBE.Clone("DataQCDBEL")
DataTOTALBEH=DataBE.Clone("DataTOTALBEH")
DataTOTALBEL=DataBE.Clone("DataTOTALBEL")

for ibin in range(0,DataBE.GetNbinsX()):
	#PtScaleup=(TTmcFSScaleUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#Q2Scaleup=(TTmcFSQ2ScaleUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#PtSmearup=(TTmcFSPtSmearUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#EtaSmearup=(TTmcFSEtaSmearUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#Triggerup=(TTmcFSTriggerUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#Btaggingup=(TTmcFSBUp.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))

	#ptup=(TTmcFSptup.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))

	#PtScaledown=(TTmcFSScaleDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#Q2Scaledown=(TTmcFSQ2ScaleDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#PtSmeardown=(TTmcFSPtSmearDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#EtaSmeardown=(TTmcFSEtaSmearDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#Triggerdown=(TTmcFSTriggerDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
	#Btaggingdown=(TTmcFSBDown.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))
	#ptdown=(TTmcFSptdown.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))

	#ups = [PtScaleup,Q2Scaleup,PtSmearup,EtaSmearup,Triggerup,Btaggingup,ptup]
	#downs = [PtScaledown,Q2Scaledown,PtSmeardown,EtaSmeardown,Triggerdown,Btaggingdown,ptdown]
	
	#upstr = ["PtScaleup","Q2Scaleup","PtSmearup","EtaSmearup","Triggerup","Btaggingup","ptup"]
	#downstr = ["PtScaledown","Q2Scaledown","PtSmeardown","EtaSmeardown","Triggerdown","Btaggingdown","ptdown"]

	sigsqup = 0.
	sigsqdown = 0.

	#for i in range(0,len(ups)):
	#	upsig = max(ups[i],downs[i],0.)
	#	downsig = min(ups[i],downs[i],0.)
	#	sigsqup+=upsig*upsig
	#	sigsqdown+=downsig*downsig

	#CrossSection=0.19*TTmcFS.GetBinContent(ibin)
	TTstat=TTmcFS.GetBinError(ibin)
	if DataBE.GetBinContent(ibin)>0:
		QCDstat=DataBE.GetBinError(ibin)
	else:
		QCDstat=0.
	QCDfit=abs(BEfiterrh.GetBinContent(ibin))
	QCDfit1=abs((DataBEh.GetBinContent(ibin)-DataBEl.GetBinContent(ibin))/2)
	QCDfit2=abs(DataBE2d.GetBinContent(ibin)-DataBE.GetBinContent(ibin))

	print ""
	print ibin
	print QCDfit
	print QCDfit1
	print QCDfit2

	QCDsys=math.sqrt(QCDfit*QCDfit + QCDfit1*QCDfit1 + QCDfit2*QCDfit2)
	QCDerror=math.sqrt(QCDstat*QCDstat+QCDsys*QCDsys)
	TTerrorup=math.sqrt(sigsqup+TTstat*TTstat)
	TTerrordown=math.sqrt(sigsqdown+TTstat*TTstat)
	Totalerrorup=math.sqrt(QCDerror*QCDerror+TTerrorup*TTerrorup)
	Totalerrordown=math.sqrt(QCDerror*QCDerror+TTerrordown*TTerrordown)
	DataQCDBEH.SetBinContent(ibin,DataQCDBEH.GetBinContent(ibin)+QCDerror)
	DataQCDBEL.SetBinContent(ibin,DataQCDBEL.GetBinContent(ibin)-QCDerror)
	DataTOTALBEH.SetBinContent(ibin,DataTOTALBEH.GetBinContent(ibin)+Totalerrorup)
	DataTOTALBEL.SetBinContent(ibin,DataTOTALBEL.GetBinContent(ibin)-Totalerrordown)

print "QCD total error"
print (DataQCDBEH.Integral()-DataBE.Integral())/DataBE.Integral()
print 
DataQCDBEH.Write()
DataQCDBEL.Write()

DataTOTALBEH.Write()
DataTOTALBEL.Write()

DataTOTALBEL.Add(TTmcFS)
DataTOTALBEH.Add(TTmcFS)
#DataTOTALBEL.Add(singletop)
#DataTOTALBEH.Add(singletop)

# Uncomment later
#for ifile in range(0,len(stops)):
#	DataTOTALBEL.Add(shists[ifile])
#	DataTOTALBEH.Add(shists[ifile])		

DataBE.SetFillColor(kYellow)
TTmcFS.SetFillColor(kRed)

DataTOTALBEH.SetLineColor(kBlue)
DataTOTALBEH.SetLineWidth(2)
#DataTOTALBEH.SetLineStyle(2)

centerqcd = DataTOTALBEL.Clone("centerqcd")
centerqcd.SetFillColor(kYellow)
centerqcd.Add(TTmcFS,-1)
centerqcd.Add(singletop,-1)

DataTOTALBEL.SetLineColor(kBlue)
DataTOTALBEL.SetLineWidth(2)
#DataTOTALBEL.SetFillColor(0)
#DataTOTALBEL.SetLineStyle(0)
#DataTOTALBEL.SetLineWidth(2)
#DataTOTALBEL.SetLineStyle(2)

sigst= ROOT.THStack("sigst", "sigst")
sigma = DataTOTALBEH.Clone("sigma")
sigma.SetFillStyle(3245)
sigma.SetFillColor(1)
sigma.SetLineColor(0)
centerqcd.SetLineColor(kYellow)

sigma.Add(DataTOTALBEL,-1)
sigst.Add(singletop)
sigst.Add(TTmcFS)
sigst.Add(centerqcd)
sigst.Add(sigma)


st1.Add(TTmcFS)
st1.Add(DataBE)

bkgline=st1.GetStack().Last().Clone("bkgline")
bkgline.SetFillColor(0)
bkgline.SetFillStyle(0)

#leg.AddEntry( DataFS, 'Data', 'P')
leg.AddEntry( DataFS, 'b mass prediction', 'P')
leg.AddEntry( DataBE, 'QCD background prediction', 'F')
leg.AddEntry( TTmcFS, 't#bar{t} MC prediction', 'F')
#leg.AddEntry( singletop, 'Single top quark MC prediction', 'F')
leg.AddEntry( sigma, '1 #sigma background uncertainty', 'F')
leg.AddEntry( sigh[0], 'W`_{R} at 1300 GeV', 'L')
leg.AddEntry( sigh[1], 'W`_{R} at 2000 GeV', 'L')
leg.AddEntry( sigh[2], 'W`_{R} at 2700 GeV', 'L')

#c1.cd()
#c1.SetLeftMargin(0.17)
#st1.GetXaxis().SetRangeUser(0,3000)
st1.SetMaximum(DataTOTALBEH.GetMaximum() * 1.3)
st1.SetMinimum(1.0)
st1.SetTitle(';M_{tb} (GeV);Counts per 100 GeV')
st1.Draw("hist")
gPad.SetLeftMargin(.16)
st1.GetYaxis().SetTitleOffset(0.9)
#DataTOTALBEH.Draw("histsame")
#DataTOTALBEL.Draw("histsame")
sigst.Draw("samehist")
bkgline.Draw("samehist")
sigh[0].Draw("samehist")
sigh[1].Draw("samehist")
sigh[2].Draw("samehist")


DataFS1	    = TH1D("DataFS1",     "mass W' in b+1",     	  	      140, 500, 4000 )
DataFS1.Rebin(rebin)
for ibin in range(1,DataFS.GetNbinsX()+1):
	DataFS1.SetBinContent(ibin,DataFS.GetBinContent(ibin))

DataFS1.SetBinErrorOption(DataFS1.kPoisson)
DataFS1 = DataFS
DataFS1.Draw("samepE")

leg.Draw()
prelim = TLatex()
prelim.SetNDC()


#insertlogo( main, 2, 11 )


prelim.DrawLatex( 0.5, 0.91, "#scale[0.8]{CMS Preliminary, 13 TeV, 10 fb^{-1}}" )
sub.cd()
gPad.SetLeftMargin(.16)
totalH = st1.GetStack().Last().Clone("totalH")
#totalH.Add(TTmcFS)
pull = Make_Pull_plot( DataFS1,totalH,DataTOTALBEH,DataTOTALBEL )


	
#pull.GetXaxis().SetRangeUser(0,3000)
pull.SetFillColor(kBlue)
pull.SetTitle(';M_{tb} (GeV);(Data-Bkg)/#sigma')
pull.SetStats(0)


pull.GetYaxis().SetRangeUser(-2.9,2.9)
pull.GetXaxis().SetLabelSize(0.05)
pull.GetYaxis().SetLabelSize(0.05)


LS = .13

pull.GetYaxis().SetTitleOffset(0.4)
pull.GetXaxis().SetTitleOffset(0.9)
pull.SetStats(0)
    

pull.GetYaxis().SetLabelSize(LS)
pull.GetYaxis().SetTitleSize(LS)
pull.GetYaxis().SetNdivisions(306)
pull.GetXaxis().SetLabelSize(LS)
pull.GetXaxis().SetTitleSize(LS)

pull.Draw("hist")

line2=ROOT.TLine(500.0,0.0,4000.0,0.0)
line2.SetLineColor(0)
line1=ROOT.TLine(500.0,0.0,4000.0,0.0)
line1.SetLineStyle(2)

line2.Draw()
line1.Draw()
gPad.Update()

main.RedrawAxis()

c1.Print('plots/MtbvsBkg_BifPoly_fit_PSET_'+options.cuts+'.root', 'root')
c1.Print('plots/MtbvsBkg_BifPoly_fit_PSET_'+options.cuts+'.pdf', 'pdf')
c1.Print('plots/MtbvsBkg_BifPoly_fit_PSET_'+options.cuts+'.png', 'png')
main.SetLogy()
st1.SetMaximum( DataBEh.GetMaximum() * 5000 )
st1.SetMinimum( 0.1)
main.RedrawAxis()

c1.Print('plots/MtbvsBkgsemilog_BifPoly_fit_PSET_'+options.cuts+'.root', 'root')
c1.Print('plots/MtbvsBkgsemilog_BifPoly_fit_PSET_'+options.cuts+'.pdf', 'pdf')
c1.Print('plots/MtbvsBkgsemilog_BifPoly_fit_PSET_'+options.cuts+'.png', 'png')

