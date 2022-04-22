# time parameters
no_intervals = 144
no_periods = 48
time_out = None

# time dictionary key names
k_no_intervals = "no_intervals"
k_no_periods = "no_periods"
k_no_intervals_periods = "no_intervals_periods"
k_time_out = "time_out"

# household task parameters
# new_households = True
create_new_households = False
no_households = 10
min_full_flex_tasks = 5
max_full_flex_tasks = 8
min_semi_flex_tasks = 0
max_semi_flex_tasks = 0
min_fixed_tasks = 0
max_fixed_tasks = 0
no_dependent_tasks = 2
ensure_dependent = False

# household dictionary key name
k_no_households = "no_households"
k_min_full_flex_tasks = "min_full_flex_tasks"
k_max_full_flex_tasks = "max_full_flex_tasks"
k_min_semi_flex_tasks = "min_semi_flex_tasks"
k_max_semi_flex_tasks = "max_semi_flex_tasks"
k_min_fixed_tasks = "min_fixed_tasks"
k_max_fixed_tasks = "max_fixed_tasks"
k_no_dependent_tasks = "no_dependent_tasks"
k_ensure_dependent = "ensure_dependent"

# objective weights
inconvenience_weight = 1
par_weight = 1

# objective dictionary key names
k_inconvenience_weight = "inconvenience_weight"
k_par_weight = "par_weight"

# multipliers
maximum_demand_multiplier = 1
max_care_f = 10

# multiplier dictionary key names
k_max_demand_multiplier = "max_demand_multiplier"
k_max_care_f = "max_care_f"

# battery parameters
max_battery_capacity = 3000  # Wh
min_battery_capacity = 0
battery_power = 3000  # 1C battery
fully_charge_hour = 0
battery_efficiency = 1

# battery dictionary key names
k_max_battery_capacity = "max_battery_capacity"
k_min_battery_capacity = "min_battery_capacity"
k_battery_power = "battery_power"
k_fully_charge_hour = "fully_charge_hour"
k_battery_efficiency = "battery_efficiency"

# pricing related parameters
pricing_table_weight = 1
# cost_function_type = "linear"
cost_function_type = "piece-wise"
min_step = 0.0005
roundup_step_digits = 1000

# tasks solver related parameters
variable_selection = "smallest"
value_choice = "indomain_min"
tasks_model_type = "pre"
tasks_solver_type = "cp"
tasks_solver_name = "gecode"

# battery solver related parameters
battery_solver_type = "mip"
battery_solver_name = "gurobi"

# external file  parameters
parent_folder = ""
file_cp_pre = parent_folder + 'data/Household-cp-pre.mzn'
file_cp_ini = parent_folder + 'data/Household-cp.mzn'
file_mip_battery = parent_folder + 'data/Battery-mip.mzn'

file_pricing_table = parent_folder + 'data/pricing_table_0.csv'
file_household_area_folder = parent_folder + 'data/'
file_pdp = parent_folder + 'data/probability.csv'
file_demand_list = parent_folder + 'data/demands_list.csv'
result_folder = parent_folder + "results/"
file_community_pkl = "community.pkl"
file_community_meta_pkl = "community_aggregate.pkl"
file_aggregator_pkl = "aggregator.pkl"
file_pricing_table_pkl = "pricing_table.pkl"
file_experiment_pkl = "experiment.pkl"

# external file dictionary key names
k_file_preferred_demand_profile = "file_preferred_demand_profile"
k_file_list_of_devices_power = "file_list_of_devices_power"
k_init_preferred_demand_profile = "init_preferred_demand_profile"
k_list_of_devices_power = "list_of_devices_power"

# summary related parameters
k_area = "area"
k_penalty_weight = "penalty_weight"
k_households_no = "no_households"
k_tasks_no = "no_tasks"
k_cost_type = "cost_function_type"
k_iteration_no = "no_iterations"
k_dependent_tasks_no = "no_dependent_tasks"

# household dictionary keys
h_key = "key"
h_psts = "preferred_starts"
h_ests = "earliest_starts"
h_lfs = "latest_finishes"
h_durs = "durations"
h_powers = "powers"
h_cfs = "care_factors"
h_max_cf = "maximum_care_factor"
h_no_precs = "no_precedences"
h_precs = "precedents"
h_succ_delay = "succeeding_delays"
h_demand_limit = "maximum_demand"
h_incon_weight = "inconvenience_cost_weight"
h_tasks_no_ff_min = "no_fully_flexible_tasks_min"
h_tasks_no_ff_max = "no_fully_flexible_tasks_max"
h_tasks_no_sf_min = "no_semi_flexible_tasks_min"
h_tasks_no_sf_max = "no_semi_flexible_tasks_max"
h_tasks_no_fixed_min = "no_fixed_tasks_min"
h_tasks_no_fixed_max = "no_fixed_tasks_max"

b_cap_max = "battery_capacity_max"
b_cap_min = "battery_capacity_min"
b_power = "battery_power"
b_profile = "battery_profile"
b_eff = "battery_efficiency"

# demand related parameters
k_household_key = "key"
s_starts = "start_times"
s_demand = "demands"
s_demand_max = "max_demand"
s_demand_max_init = "init_max_demand"
s_demand_reduction = "demand_reduction"
s_demand_total = "total_demand"
s_par = "PAR"
s_par_init = "init_PAR"
s_final = "final"
s_penalty = "inconvenient"
s_penalty_init = "init_inconvenient"
s_obj = "objective"
s_debugger = "debugger"

# step size
p_step = "step_size"

# pricing related parameters
p_cost = "cost"
p_cost_reduction = "cost_reduction"
p_prices = "prices"
p_price_levels = "price_levels"
p_demand_table = "demand_levels"
p_par_weight = "par_weight"

# run time related
t_time = "run_time"
t_scheduling = "rescheduling_time"
t_pricing = "pricing_time"
t_average = "average_run_time"

# k1_interval = "interval"
# k1_period = "period"
m_algorithm = "algorithm"
m_minizinc = "minizinc"
m_ogsa = "ogsa"
m_mip_battery = "mip_battery"
m_before_fw = "scheduling"
m_after_fw = "pricing"

# tracking-related
k_tracker = "tracker"
k_tracker_final = "tracker_final"
k_others = "others"
algorithms = dict()
algorithms[m_minizinc] = dict()
algorithms[m_minizinc][m_before_fw] = m_minizinc
algorithms[m_minizinc][m_after_fw] = f"{m_minizinc}_fw"
algorithms[m_ogsa] = dict()
algorithms[m_ogsa][m_before_fw] = m_ogsa
algorithms[m_ogsa][m_after_fw] = f"{m_ogsa}_fw"

algorithm_full_names = dict()
algorithm_full_names[algorithms[m_minizinc][m_before_fw]] = "MiniZinc model with data preprocessing"
algorithm_full_names[algorithms[m_minizinc][m_after_fw]] = "FW-DDSM with MiniZinc model and data preprocessing"
algorithm_full_names[algorithms[m_ogsa][m_before_fw]] = "OGSA"
algorithm_full_names[algorithms[m_ogsa][m_after_fw]] = "FW-DDSM with OGSA"




