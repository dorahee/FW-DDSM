from fw_ddsm.experiment import *
from fw_ddsm.parameter import *

algorithms = dict()
# algorithms[k1_minizinc] = dict()
# algorithms[k1_minizinc][k2_before_fw] = k1_minizinc
# algorithms[k1_minizinc][k2_after_fw] = f"{k1_minizinc}_fw"
algorithms[k1_ogsa] = dict()
algorithms[k1_ogsa][k2_before_fw] = k1_ogsa
algorithms[k1_ogsa][k2_after_fw] = f"{k1_ogsa}_fw"

experiment(algorithms)