from fw_ddsm.community import *
from fw_ddsm.parameter import *

new_community = Community()
new_community.new(file_preferred_demand_profile_path=file_pdp, file_demand_list_path=file_demand_list,
                  scheduling_method=k1_ogsa, write_to_file_path="community")
new_community.new(file_preferred_demand_profile_path=file_pdp, file_demand_list_path=file_demand_list,
                  scheduling_method=k1_minizinc, write_to_file_path="community")
# new_community.read(scheduling_method=k1_ogsa, read_from_folder="community")
print()
