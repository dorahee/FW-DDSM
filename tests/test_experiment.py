from fw_ddsm.parameter import *
from fw_ddsm.experiment import *

algorithms = dict()
# algorithms[k1_minizinc] = dict()
# algorithms[k1_minizinc][k2_before_fw] = k1_minizinc
# algorithms[k1_minizinc][k2_after_fw] = f"{k1_minizinc}_fw"
algorithms[k1_ogsa] = dict()
algorithms[k1_ogsa][k2_before_fw] = k1_ogsa
algorithms[k1_ogsa][k2_after_fw] = f"{k1_ogsa}_fw"

new_experiment = Experiment(algorithms=algorithms, num_households=2000)
new_experiment.new_data(max_demand_multiplier=maxium_demand_multiplier,
                        num_tasks_dependent=3,
                        full_flex_task_min=3, full_flex_task_max=0,
                        semi_flex_task_min=3, semi_flex_task_max=0,
                        fixed_task_min=5, fixed_task_max=0,
                        inconvenience_cost_weight=1, max_care_factor=care_f_max)
# new_experiment.read_data()

for alg in algorithms.values():
    new_experiment.iteration(alg)
    aggregate_demand_profile, total_inconvenience = new_experiment.final_schedule(alg)
    print("------------------------------")
print("Experiment is finished. ")
