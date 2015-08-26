#! /bin/sh
tar czvf tarball.tgz Wprime_Functions.py rootlogon.C TBrate.py Triggerweight_signalright2000btags.root PileUp_Ratio_ttbar.root PileUp_Ratio_signal*.root 
./development/runManySections.py --createCommandFile --cmssw --addLog --setTarball=tarball.tgz \tagrate.listOfJobs commands.cmd
./runManySections.py --submitCondor commands.cmd
condor_q jlee2
