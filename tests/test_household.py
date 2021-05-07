from fw_ddsm.household import *

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

prices = [1] * 16 + [1000, 1000] + [1] * 126

# test_household.schedule(num_iteration=no_intervals, prices=prices)
test_household.schedule(num_iteration=1, prices=prices, use_battery=True, battery_solver="gurobi")
print(0)
