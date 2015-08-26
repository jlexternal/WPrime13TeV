#! /bin/sh
tar czvf tarball.tgz fitdata Tagrate2D.root rootlogon.C TBanalyzer.py Wprime_Functions.py TBrate.py TRIG_EFFICWPHTdata_dijet8TeV.root PileUp_Ratio_ttbar.root PileUp_Ratio_signal*.root 

./development/runManySections.py --createCommandFile --cmssw --addLog --setTarball=tarball.tgz \ana.listOfJobs commands.cmd
./runManySections.py --submitCondor commands.cmd
condor_q knash
