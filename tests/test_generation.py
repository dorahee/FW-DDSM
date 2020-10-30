from household import generation
from numpy import genfromtxt
from fw_ddsm.parameter import *

preferred_demand_profile = genfromtxt(file_probability, delimiter=',', dtype="float")
list_of_devices_power = genfromtxt(file_demand_list, delimiter=',', dtype="float")
algorithms = dict()
# algorithms[k1_optimal] = dict()
# algorithms[k1_optimal][k2_before_fw] = k1_optimal
# algorithms[k1_optimal][k2_after_fw] = f"{k1_optimal}_fw"
algorithms[k1_heuristic] = dict()
algorithms[k1_heuristic][k2_before_fw] = k1_heuristic
algorithms[k1_heuristic][k2_after_fw] = f"{k1_heuristic}_fw"

household = generation.new_household(preferred_demand_profile, list_of_devices_power,
                                     full_flex_task_min=3, semi_flex_task_min=3, fixed_task_min=5,
                                     num_tasks_dependent=3, write_to_file_path="test/")

household = generation.existing_household("test/household0.txt")
print(household)

households, demand_profile = generation.new_households(10, algorithms, file_probability, file_demand_list,
                                                       write_to_file_path="test2/")

# households, community_tracks = generation.existing_community("test2/", inconvenience_cost_weight=None)

print(households, demand_profile)

pricing_table = generation.new_pricing_table(file_pricing_table, 1)
print(pricing_table)

