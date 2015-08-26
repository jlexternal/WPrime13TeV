# -*- coding: utf-8 -*-
import scipy.interpolate

def build_allhad_model():
    files = ['Limits_allhadronic_right_PSET_default.root']
    #files = ['allhadronic_mttbar.root']
    model = build_model_from_rootfile(files,  include_mc_uncertainties=True)
    model.fill_histogram_zerobins()
    model.set_signal_processes('wp*')
    for p in model.processes:

       		model.add_lognormal_uncertainty('overall', math.log(1.25), 'ttbar')
   
    return model

def limits_allhad(model, step = 1):
   if step==0:

       runs = bayesian_quantiles(model,'toys:0',1000, run_theta=False)
       print runs
       for point in runs:
		runs[point].get_configfile(Options())
       return
   expected_limits = bayesian_quantiles(model, 'toys:0', 1000, run_theta=True)
   plot_expected = limit_band_plot(expected_limits, True)
   observed_limits = bayesian_quantiles(model, 'data', 10)
   plot_observed = limit_band_plot(observed_limits, False)
   report_limit_band_plot(plot_expected, plot_observed, 'Bayesian', 'bayesian')

   plot_expected.write_txt('bayesian_expected_allhad_limits_WprimeR.txt')
   plot_observed.write_txt('bayesian_observed_allhad_limits_WprimeR.txt')

   report.write_html('htmlout_WprimeR_allhad')


model = build_allhad_model()


for p in model.distribution.get_parameters():
    d = model.distribution.get_distribution(p)
    if d['typ'] == 'gauss' and d['mean'] == 0.0 and d['width'] == 1.0:
        model.distribution.set_distribution_parameters(p, range = [-5.0, 5.0])


limits_allhad(model,RSTEP)

