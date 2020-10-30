# time related parameters
no_intervals = 144
no_periods = 48
no_intervals_periods = int(no_intervals / no_periods)

# household related parameters
# new_households = True
new_households = False
no_households = 100
no_tasks_min = 5
no_tasks_max = 8
no_tasks_dependent = 2
maxium_demand_multiplier = no_tasks_min
care_f_max = 10
care_f_weight = 0

# pricing related parameters
pricing_table_weight = 1
# cost_function_type = "linear"
cost_function_type = "piece-wise"


# solver related parameters
var_selection = "smallest"
val_choice = "indomain_min"
model_type = "pre"
solver_type = "cp"

# external file related parameters
parent_folder = ""
file_cp_pre = parent_folder + 'models/Household-cp-pre.mzn'
file_cp_ini = parent_folder + 'models/Household-cp.mzn'
file_pricing_table = parent_folder + 'data/pricing_table_0.csv'
file_household_area_folder = parent_folder + 'data/'
file_probability = parent_folder + 'data/probability.csv'
file_demand_list = parent_folder + 'data/demands_list.csv'
result_folder = parent_folder + "results/"

# summary related parameters
k0_area = "area"
k0_penalty_weight = "penalty_weight"
k0_households_no = "no_households"
k0_tasks_no = "no_tasks"
k0_cost_type = "cost_function_type"
k0_iteration_no = "no_iterations"

# househole dictionary keys
h_psts = "preferred_starts"
h_ests = "earliest_starts"
h_lfs = "latest_finishes"
h_durs = "durations"
h_powers = "powers"
h_cfs = "care_factors"
h_no_precs = "no_precedences"
h_precs = "precedents"
h_succ_delay = "succeeding_delays"
h_max_demand = "maximum_demand"
h_demand_profile = "demand_profile"
h_incon_weight = "inconvenience_cost_weight"


# demand related parameters
k0_household_key = "key"
k0_starts = "start_times"
k0_demand = "demands"
k0_demand_max = "max_demand"
k0_demand_total = "total_demand"
k0_par = "PAR"
k0_final = "final"

# step size
k0_step = "step_size"

# objective related parameters
k0_cost = "cost"
k0_penalty = "inconvenient"
k0_obj = "objective"

# pricing related parameters
k0_prices = "prices"
k0_price_levels = "price_levels"
k0_demand_table = "demand_levels"

# run time related
k0_time = "run_time"
k1_time_scheduling = "rescheduling_time"
k1_time_pricing = "pricing_time"
k1_time_average = "average_run_time"

# k1_interval = "interval"
# k1_period = "period"
k0_algorithm = "algorithm"
k1_optimal = "optimal"
k1_heuristic = "heuristic"
k2_scheduling = "scheduling"
k2_pricing = "pricing"

