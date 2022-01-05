from household import *

test_household = Household()

# test_household.new(num_intervals=no_intervals,
#                    preferred_demand_profile_csv=file_pdp,
#                    list_of_devices_power_csv=file_demand_list,
#                    scheduling_method=m_ogsa,
#                    write_to_folder="households")
# test_household.read_household(scheduling_method=m_ogsa,
#                               read_from_folder="households")
#
# test_household.new(num_intervals=no_intervals,
#                    preferred_demand_profile_csv=file_pdp,
#                    list_of_devices_power_csv=file_demand_list,
#                    tasks_scheduling_method=m_minizinc,
#                    write_to_folder="households")
test_household.read_household(tasks_scheduling_method=m_minizinc,
                              read_from_folder="households")

# prices = [141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141,
#           141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141,
#           141, 141, 141, 143, 143, 143, 142, 142, 142, 148, 148, 148,
#           189, 189, 189, 163, 163, 163, 145, 145, 145, 141, 141, 141,
#           141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141,
#           141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141, 141,
#           141, 141, 141, 141, 141, 141, 142, 142, 142, 141, 141, 141,
#           146, 146, 146, 158, 158, 158, 203, 203, 203, 313, 313, 313,
#           1155, 1155, 1155, 1155, 1155, 1155, 1446, 1446, 1446, 616, 616, 616,
#           616, 616, 616, 363, 363, 363, 363, 363, 363, 313, 313, 313,
#           427, 427, 427, 221, 221, 221, 158, 158, 158, 148, 148, 148,
#           158, 158, 158, 143, 143, 143, 142, 142, 142, 144, 144, 144]

prices = [80.02, 82.14, 72.03, 66.82, 66.46, 65.47, 65.01, 65.23, 69.37, 71.96, 78.22, 80.45, 92.63, 95.31, 84.96,
          89.32, 91.59, 93.18, 90.95, 92.65, 91.99, 99.84, 102.75, 101.39, 100.76, 97.71, 101.62, 182.04, 578.54,
          175.48, 172.56, 302.27, 440.98, 942.68, 656.85, 331.4, 98.96, 96.81, 96.92, 93.1, 92.29, 87.82, 85, 79.04,
          83.4, 79.05, 92.52, 94.28]

# test_household.schedule(num_iteration=no_intervals, prices=prices)
test_household.schedule(num_iteration=1, prices=prices, use_battery=True, battery_solver="gurobi")
print(0)
