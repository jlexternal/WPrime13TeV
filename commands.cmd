# -*- sh -*- # for font lock mode
# variable definitions
- env = cd /uscms_data/d3/jlee2/WPrime13TeV/CMSSW_7_4_1/src/Wprime13TeV; eval `scramv1 runtime -sh`; cd -
- tag = 
- output = outputFile=
- tagmode = none
- tarfile = /uscms_data/d3/jlee2/WPrime13TeV/CMSSW_7_4_1/src/Wprime13TeV/tarball.tgz
- untardir = tardir
- copycommand = cp

# Sections listed
output_$(JID)        python ./tardir/TBanalyzer.py -s ttbar -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s ttbar -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s ttbar -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s ttbar -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s ttbar -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT300 -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT300 -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT300 -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT300 -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT300 -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT470 -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT470 -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT470 -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT470 -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT470 -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT600 -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT600 -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT600 -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT600 -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT600 -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT800 -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT800 -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT800 -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT800 -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT800 -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1000 -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1000 -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1000 -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1000 -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1000 -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1400 -n 1 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1400 -n 2 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1400 -n 3 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1400 -n 4 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s QCDPT1400 -n 5 -j 5 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s signalright1300 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s signalright2000 -c default -g on
output_$(JID)        python ./tardir/TBanalyzer.py -s signalright2700 -c default -g on
