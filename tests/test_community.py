from common.parameter import *

new_community = Community()
# new_community.new(file_preferred_demand_profile=file_pdp, file_demand_list=file_demand_list,
#                   tasks_scheduling_method=m_ogsa, write_to_file_path="community")
new_community.new(file_preferred_demand_profile=file_pdp, file_demand_list=file_demand_list,
                  tasks_scheduling_method=m_minizinc)
new_community.read(tasks_scheduling_method=m_minizinc)

prices = [80.02, 82.14, 72.03, 66.82, 66.46, 65.47, 65.01, 65.23, 69.37, 71.96, 78.22, 80.45, 92.63, 95.31, 84.96,
          89.32, 91.59, 93.18, 90.95, 92.65, 91.99, 99.84, 102.75, 101.39, 100.76, 97.71, 101.62, 182.04, 578.54,
          175.48, 172.56, 302.27, 440.98, 942.68, 656.85, 331.4, 98.96, 96.81, 96.92, 93.1, 92.29, 87.82, 85, 79.04,
          83.4, 79.05, 92.52, 94.28]
new_community.schedule(num_iteration=1, prices=prices,
                       tasks_scheduling_method=m_minizinc
                       # , use_battery=True, battery_solver="gurobi"
                       )
prob = [0.5, 0.5]
new_community.finalise_schedule(start_probability_distribution=prob)
print()
