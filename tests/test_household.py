from fw_ddsm.household import *

test_household = Household()
test_household.new(num_intervals=no_intervals,
                   preferred_demand_profile_csv=file_pdp,
                   list_of_devices_power_csv=file_demand_list,
                   scheduling_method=m_ogsa,
                   write_to_folder="households")
test_household.new(num_intervals=no_intervals,
                   preferred_demand_profile_csv=file_pdp,
                   list_of_devices_power_csv=file_demand_list,
                   scheduling_method=m_minizinc,
                   write_to_folder="households")

# test_household.read(scheduling_method=k1_ogsa,
#                     read_from_file="households/household0.txt")
print(0)
