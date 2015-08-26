rm *limits*.txt
cp limitsetting/theta/*.txt ./
cat *observed*.txt | grep -v "# x; y" >./observed_limits.txt
cat *expected*.txt | grep -v "# x; y; band 0 low; band 0 high; band 1 low; band 1 high" >./expected_limits.txt
python limit_plot_shape.py --inputFileExp=expected_limits.txt --inputFileObs=observed_limits.txt --useLog --outputName=comb


