

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

st1 = ROOT.THStack("st1", "st1")		

leg = TLegend(0.60, 0.35, 0.84, 0.84)
leg.SetFillColor(0)
leg.SetBorderSize(0)

leg1 = TLegend(0.45, 0.5, 0.84, 0.84)
leg1.SetFillColor(0)
leg1.SetBorderSize(0)


leg2 = TLegend(0.5, 0.5, 0.84, 0.84)
leg2.SetFillColor(0)
leg2.SetBorderSize(0)

Mult = 1.0  

#Kfac=1.2

########--------Declare values of the different variables here in an array that the loop will call upon--------########

kVar = ["bmass", "teta", "beta", "tpt", "bpt", "sumpt", "tphi", "bphi", "dphi"]

rebin = [3, 2, 2, 5, 5, 5, 2, 2, 2]    ##divides original amount of bins by this factor

plotTitle = ["Mass of b (GeV)", "Top Candidate #eta", "b Candidate #eta", "Top Candidate p_{t} (GeV)", "b Candidate p_{t} (GeV)",
		"p_{t} (GeV) of tb System", "Top Candidate #phi (rad)", "b Candidate #phi (rad)", 
		"|#delta #phi| between Top and b Candidates"]


sigf = [
ROOT.TFile("rootfiles/TBkinematicsweightedsignalright1300_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+options.cuts+".root"),
ROOT.TFile("rootfiles/TBkinematicsweightedsignalright2000_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+options.cuts+".root"),
ROOT.TFile("rootfiles/TBkinematicsweightedsignalright2700_Trigger_HLT_AK8DiPFJet280_200_TrimMass30_BTagCSV0p41_v1,HLT_PFHT900_v1_none_PSET_"+options.cuts+".root")
]

########--------Loop through the multiple variables and create histograms for each--------########



for index in range(0,9):

	print "Running loop for kVar = " + kVar[index]
	sigh = [
	sigf[0].Get("HG_"+str(kVar[index])),
	sigf[1].Get("HG_"+str(kVar[index])),
	sigf[2].Get("HG_"+str(kVar[index]))
	]

	sigh[0].Scale(Mult)
	sigh[1].Scale(Mult)
	sigh[2].Scale(Mult)

	sigh[0].Rebin(rebin[index])
	sigh[1].Rebin(rebin[index])
	sigh[2].Rebin(rebin[index])

	sigh[0].SetLineStyle(5)
	sigh[1].SetLineStyle(6)
	sigh[2].SetLineStyle(7)

	sigh[0].SetLineWidth(2)
	sigh[1].SetLineWidth(2)
	sigh[2].SetLineWidth(2)

	sigh[0].SetLineColor(1)
	sigh[1].SetLineColor(2)
	sigh[2].SetLineColor(4)	

	stops = ['singletop_s','singletop_sB','singletop_t','singletop_tB','singletop_tW','singletop_tWB'] #what is this

	sfiles=[]
	shists=[]
	ssubs=[]
	ssubsh=[]
	ssubsl=[]


	DataB11 = ROOT.TFile("rootfiles/TBkinematicsQCD_PSET_"+options.cuts+"weighted.root")
	TTmc = ROOT.TFile("rootfiles/TBkinematicsttbar_PSET_"+options.cuts+"weighted.root")


	DataFS = DataB11.Get("HG_"+str(kVar[index]))
	DataFS.Add(TTmc.Get("HG_"+str(kVar[index])))

	DataBE = DataB11.Get("QCDbkg_"+str(kVar[index]))
	#DataBE.Add(TTmc.Get("QCDbkg_"+str(kVar[index])))

	DataBE2d = DataB11.Get("QCDbkg2D_" +str(kVar[index]))

	
	c1 = TCanvas('c1', 'Data Full selection vs b pt tagging background', 900, 700)  

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
	
	
	

	#TTmcScaleUp = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_nominal_ScaleUp_PSET_"+options.cuts+".root")
	#TTmcScaleDown = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_nominal_ScaleDown_PSET_"+options.cuts+".root")

	#TTmcPtSmearUp = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_nominal_PtSmearUp_PSET_"+options.cuts+".root")
	#TTmcPtSmearDown = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_nominal_PtSmearDown_PSET_"+options.cuts+".root")

	#TTmcQ2ScaleUp = ROOT.TFile("rootfiles/TBkinematicsttbarscaleup_Trigger_nominal_none_PSET_"+options.cuts+".root")
	#TTmcQ2ScaleDown = ROOT.TFile("rootfiles/TBkinematicsttbarscaledown_Trigger_nominal_none_PSET_"+options.cuts+".root")

	#TTmcEtaSmearUp = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_nominal_EtaSmearUp_PSET_"+options.cuts+".root")
	#TTmcEtaSmearDown = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_nominal_EtaSmearDown_PSET_"+options.cuts+".root")

	#TTmcTriggerUp = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_up_none_PSET_"+options.cuts+".root")
	#TTmcTriggerDown = ROOT.TFile("rootfiles/TBkinematicsttbar_Trigger_down_none_PSET_"+options.cuts+".root")

	output = ROOT.TFile( "TBkinematics_output_PSET_"+options.cuts+".root", "recreate" )
	output.cd()

	TTmcFS = TTmc.Get("HG_"+str(kVar[index]))

	TTmcBE = TTmc.Get("QCDbkg_" + str(kVar[index]))
	TTmcBE2d = TTmc.Get("QCDbkg2D_" + str(kVar[index]))


	TTmcBEh = TTmc.Get("QCDbkgh_" + str(kVar[index]))
	DataBEh = DataB11.Get("QCDbkgh_" + str(kVar[index]))

	TTmcBEl = TTmc.Get("QCDbkgl_" + str(kVar[index]))
	DataBEl = DataB11.Get("QCDbkgl_" + str(kVar[index]))

	#TTmcFSScaleUp = TTmcScaleUp.Get(kVar[index])
	#TTmcFSScaleDown = TTmcScaleDown.Get(kVar[index])

	#TTmcFSQ2ScaleUp = TTmcQ2ScaleUp.Get(kVar[index])
	#TTmcFSQ2ScaleDown = TTmcQ2ScaleDown.Get(kVar[index])

	#TTmcFSPtSmearUp = TTmcPtSmearUp.Get(kVar[index])
	#TTmcFSPtSmearDown = TTmcPtSmearDown.Get(kVar[index])

	#TTmcFSEtaSmearUp = TTmcEtaSmearUp.Get(kVar[index])
	#TTmcFSEtaSmearDown = TTmcEtaSmearDown.Get(kVar[index])

	#TTmcFSTriggerUp = TTmcTriggerUp.Get(kVar[index])
	#TTmcFSTriggerDown = TTmcTriggerDown.Get(kVar[index])

	##These are not necessary for this file - JL

	#TTmcFSBUp =  TTmc.Get("MtbBup")
	#TTmcFSBDown =  TTmc.Get("MtbBDown")

	#TTmcFSptup =  TTmc.Get("Mtbptup")
	#TTmcFSptdown =  TTmc.Get("Mtbptdown")

	#TTmcFSBUp.Rebin(rebin[index])
	#TTmcFSBDown.Rebin(rebin[index])

	#TTmcFSptup.Rebin(rebin[index])
	#TTmcFSptdown.Rebin(rebin[index])

	#TTmcFSQ2ScaleUp.Rebin(rebin[index])
	#TTmcFSQ2ScaleDown.Rebin(rebin[index])

	#TTmcFSScaleUp.Rebin(rebin[index])
	#TTmcFSScaleDown.Rebin(rebin[index])

	#TTmcFSPtSmearUp.Rebin(rebin[index])
	#TTmcFSPtSmearDown.Rebin(rebin[index])

	#TTmcFSEtaSmearUp.Rebin(rebin[index])
	#TTmcFSEtaSmearDown.Rebin(rebin[index])

	#TTmcFSTriggerUp.Rebin(rebin[index])
	#TTmcFSTriggerDown.Rebin(rebin[index])


	DataBE2d.Rebin(rebin[index])
	TTmcFS.Rebin(rebin[index])
	DataBE.Rebin(rebin[index])
	DataFS.Rebin(rebin[index])
	print DataFS.Integral()
	DataBEl.Rebin(rebin[index])
	DataBEh.Rebin(rebin[index])
	TTmcBE.Rebin(rebin[index])
	TTmcBE2d.Rebin(rebin[index])
	TTmcBEh.Rebin(rebin[index])
	TTmcBEl.Rebin(rebin[index])

	#subtract weighted TT pretags

	unsubbkg = DataBE.Clone()
	
	#uncomment later
	#DataBE.Add(TTmcBE,-1)
	#DataBE2d.Add(TTmcBE2d,-1)
	#DataBEl.Add(TTmcBE,-1)
	#DataBEh.Add(TTmcBE,-1)

	if False:
		for ifile in range(0,len(stops)):
			sfiles.append(ROOT.TFile("rootfiles/TBkinematicsweighted"+stops[ifile]+"_Trigger_nominal_none_PSET_"+options.cuts+".root"))
			shists.append(sfiles[ifile].Get("HG_"+str(kVar[index])))
			ssubs.append(sfiles[ifile].Get("QCDbkg_" + kVar[index]))

			ssubsh.append(sfiles[ifile].Get("QCDbkgh_" + kVar[index]))
			ssubsl.append(sfiles[ifile].Get("QCDbkgl_" + kVar[index]))
			shists[ifile].Rebin(rebin[index])
			ssubs[ifile].Rebin(rebin[index])
			ssubsh[ifile].Rebin(rebin[index])
			ssubsl[ifile].Rebin(rebin[index])
			#print str((Luminosity*stopxsecs[ifile]*TeffScale)/stopnevents[ifile])    
			DataBE.Add(ssubs[ifile],-1)
			DataBEl.Add(ssubsl[ifile],-1)	
			DataBEh.Add(ssubsh[ifile],-1)  
			if ifile == 0:
				singletop = shists[ifile]
				schanst = shists[ifile]	
			
			if ifile >0:
				singletop.SetFillColor(6)
				singletop.Add(shists[ifile])
				if ifile<=1:
					schanst.Add(shists[ifile])
	
	#st1.Add(singletop)
	

	output.cd()

	fittitles = ["pol0","pol2","pol3","FIT","Bifpoly","expofit"]
	QCDbkg_ARR = []
	for ihist in range(0,len(fittitles)):
		QCDbkg_ARR.append(DataB11.Get("QCDbkg_"+kVar[index]+str(fittitles[ihist])).Rebin(rebin[index]))  ##edited this part, but need confirmation

	BEfiterrh = Fit_Uncertainty(QCDbkg_ARR)

	output.cd()
	DataQCDBEH=DataBE.Clone("DataQCDBEH")
	DataQCDBEL=DataBE.Clone("DataQCDBEL")
	DataTOTALBEH=DataBE.Clone("DataTOTALBEH")
	DataTOTALBEL=DataBE.Clone("DataTOTALBEL")

	for ibin in range(0,DataBE.GetNbinsX()+1):
		#PtScaleup=(TTmcFSScaleUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#Q2Scaleup=(TTmcFSQ2ScaleUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#PtSmearup=(TTmcFSPtSmearUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#EtaSmearup=(TTmcFSEtaSmearUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#Triggerup=(TTmcFSTriggerUp.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#Btaggingup=(TTmcFSBUp.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))			##Need to change this - JL
		#ptup=(TTmcFSptup.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))			##Need to change this - JL	

		#PtScaledown=(TTmcFSScaleDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#Q2Scaledown=(TTmcFSQ2ScaleDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#PtSmeardown=(TTmcFSPtSmearDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#EtaSmeardown=(TTmcFSEtaSmearDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#Triggerdown=(TTmcFSTriggerDown.GetBinContent(ibin) -TTmcFS.GetBinContent(ibin))
		#Btaggingdown=(TTmcFSBDown.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))		##Need to change this - JL
		#ptdown=(TTmcFSptdown.GetBinContent(ibin)-TTmcFS.GetBinContent(ibin))			##Need to change this - JL

		#ups = [PtScaleup,Q2Scaleup,PtSmearup,EtaSmearup,Triggerup]
			#Btaggingup,ptup]
		#downs = [PtScaledown,Q2Scaledown,PtSmeardown,EtaSmeardown,Triggerdown]
			#Btaggingdown,ptdown]
	
		#upstr = ["PtScaleup","Q2Scaleup","PtSmearup","EtaSmearup","Triggerup"]
			 #"Btaggingup","ptup"  - These two may no longer be necessary - JL
				
		#downstr = ["PtScaledown","Q2Scaledown","PtSmeardown","EtaSmeardown","Triggerdown"]
			   #"Btaggingdown","ptdown"  - These two may no longer be necessary - JL
				

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
		#QCDfit2=0.0
		QCDsys=sqrt(QCDfit*QCDfit + QCDfit1*QCDfit1 + QCDfit2*QCDfit2)
		QCDerror= sqrt(QCDstat*QCDstat+QCDsys*QCDsys)
		print "QCD"
		print DataBE.GetBinContent(ibin)
		print "ttbar"
		print TTmcFS.GetBinContent(ibin)
		print QCDfit
		print QCDfit1
		print QCDfit2
		print ""
		TTerrorup=sqrt(sigsqup+TTstat*TTstat)
		TTerrordown=sqrt(sigsqdown+TTstat*TTstat)
		Totalerrorup=sqrt(QCDerror*QCDerror+TTerrorup*TTerrorup)
		Totalerrordown=sqrt(QCDerror*QCDerror+TTerrordown*TTerrordown)
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

	#uncomment later
	#for ifile in range(0,len(stops)):
	#	DataTOTALBEL.Add(shists[ifile])	#axis error
	#	DataTOTALBEH.Add(shists[ifile])	#axis error

	DataBE.SetFillColor(kYellow)
	TTmcFS.SetFillColor(kRed)

	DataTOTALBEH.SetLineColor(kBlue)
	DataTOTALBEH.SetLineWidth(2)
	#DataTOTALBEH.SetLineStyle(2)

	centerqcd = DataTOTALBEL.Clone("centerqcd") #top of ttbar to lower error bar on bkg
	centerqcd.SetFillColor(kRed)
	centerqcd.Add(DataBE,-1)	#axis error
	#centerqcd.Add(singletop,-1)	#axis error

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
	centerqcd.SetLineColor(kRed)

	sigma.Add(DataTOTALBEL,-1)
	#sigst.Add(singletop)
	sigst.Add(DataBE)
	sigst.Add(centerqcd)
	sigst.Add(sigma)

	st1.Add(DataBE)
	st1.Add(TTmcFS)


	bkgline=st1.GetStack().Last().Clone("bkgline")
	bkgline.SetFillColor(0)
	bkgline.SetFillStyle(0)
	if index==0:
		#leg.AddEntry( DataFS, 'Data', 'P')
		leg.AddEntry( DataFS, str(plotTitle[index])+"+ t#bar{t}", 'P')
		leg.AddEntry( DataBE, 'QCD bkg + t#bar{t} prediction', 'F')
		leg.AddEntry( TTmcFS, 't#bar{t} MC prediction', 'F')
		#leg.AddEntry( singletop, 'Single top quark MC prediction', 'F')
		leg.AddEntry( sigma, '1 #sigma background uncertainty', 'F')
		leg.AddEntry( sigh[0], 'W`_{R} at 1300 GeV', 'L')
		leg.AddEntry( sigh[1], 'W`_{R} at 2000 GeV', 'L')
		leg.AddEntry( sigh[2], 'W`_{R} at 2700 GeV', 'L')

	#c1.cd()
	#c1.SetLeftMargin(0.17)
	#st1.GetXaxis().SetRangeUser(0,3000)

	st1.SetMaximum(DataTOTALBEH.GetMaximum() * 1.5)         #sets y-axis limits for the plot axes
	st1.SetMinimum(1.0)					#sets limits for the plot axes
	st1.SetTitle(";" + plotTitle[index] + ';Counts')	#get title from preset array above 
	st1.Draw("hist")
	gPad.SetLeftMargin(.16)
	st1.GetYaxis().SetTitleOffset(0.9)
	#DataTOTALBEH.Draw("histsame")
	#DataTOTALBEL.Draw("histsame")
	sigst.Draw("samehist")	 #uncertainty hatches
	bkgline.Draw("samehist") #border for yellow
	sigh[0].Draw("samehist")
	sigh[1].Draw("samehist")
	sigh[2].Draw("samehist")


	#DataFS1	    = TH1D("DataFS1",     "mass W' in b+1", 140, 500, 4000 ) ##correct error bar
	#DataFS1.Rebin(rebin[index])
	#for ibin in range(1,DataFS.GetNbinsX()+1):
	#	DataFS1.SetBinContent(ibin,DataFS.GetBinContent(ibin))

	#DataFS1.SetBinErrorOption(DataFS1.kPoisson)
	DataFS.Draw("samepE")

	leg.Draw()
	prelim = TLatex()
	prelim.SetNDC()


	insertlogo( main, 2, 11 )

	#prelim.DrawLatex( 0.5, 0.91, "#scale[0.8]{CMS Preliminary, 8 TeV, 19.7 fb^{-1}}" )
	sub.cd()
	gPad.SetLeftMargin(.16)
	totalH = st1.GetStack().Last().Clone("totalH")
	totalH.Add(TTmcFS)
	pull = Make_Pull_plot( DataFS,totalH,DataTOTALBEH,DataTOTALBEL) #axis error


	
	#pull.GetXaxis().SetRangeUser(0,3000)
	pull.SetFillColor(kBlue)
	pull.SetTitle(";" + plotTitle[index] +';(Data-Bkg)/#sigma')
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

	c1.Print('plots/'+kVar[index]+'vsBkg_BifPoly_fit_PSET_'+options.cuts+'.root', 'root')
	c1.Print('plots/'+kVar[index]+'vsBkg_BifPoly_fit_PSET_'+options.cuts+'.pdf', 'pdf')
	c1.Print('plots/'+kVar[index]+'vsBkg_BifPoly_fit_PSET_'+options.cuts+'.png', 'png')
output.Write()
output.Close()
