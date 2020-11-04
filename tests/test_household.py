from fw_ddsm.household import *

test_household = Household()
test_household.new_household(preferred_demand_profile_csv=file_pdp,
                             list_of_devices_power_csv=file_demand_list,
                             scheduling_method=k1_ogsa,
                             write_to_file_path="households")
test_household.new_household(preferred_demand_profile_csv=file_pdp,
                             list_of_devices_power_csv=file_demand_list,
                             scheduling_method=k1_minizinc,
                             write_to_file_path="households")

# test_household.read(scheduling_method=k1_ogsa,
#                     read_from_file="households/household0.txt")
print(0)
