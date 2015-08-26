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

parser.add_option('-i', '--inputf', metavar='F', type='string', action='store',
                  default	=	'NULL_INPUT',
                  dest		=	'inputf',
                  help		=	'input root file')

parser.add_option('-w', '--weight', metavar='F', type='float', action='store',
                  default=1.0,
                  dest='weight',
                  help='weight')

parser.add_option('-o', '--outputf', metavar='F', type='string', action='store',
                  default	=	'NULL_OUTPUT',
                  dest		=	'outputf',
                  help		=	'output root file')

(options, args) = parser.parse_args()

infile = ROOT.TFile(options.inputf)
output = ROOT.TFile( options.outputf, "recreate" )

D = infile.GetListOfKeys()
weight = options.weight





try :
	reweight = array('d',[0.])
	weighttree = infile.Get('Weight')
	weighttree.SetBranchAddress("weightv",reweight)
	weighttree.GetEntry(0)
	print "reweighting. Previous weight " +  str(reweight[0])

except :
	reweight =array('d',[1.])
for i in range(0,len(D)):
	a = D[i].ReadObj()
	try:
		a.Scale(weight/reweight[0])
		a.Write()
	except:
		if  a.GetName() != 'Weight':
			print "not scaling " + str(a.GetName()) 
			a.CloneTree().Write()

weightv = array('d',[weight])
t = TTree("Weight", "Weight");
t.Branch('weightv',weightv, "weightv/D")
#weightv = weight
t.Fill()
t.Write()
output.Close()

