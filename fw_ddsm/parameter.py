# time related parameters
no_intervals = 144
no_periods = 48
no_intervals_periods = int(no_intervals / no_periods)

# household related parameters
# new_households = True
create_new_households = False
no_households = 10
no_full_flex_tasks_min = 5
no_full_flex_tasks_max = 8
no_semi_flex_tasks_min = 0
no_fixed_tasks_min = 0
no_tasks_dependent = 2
maxium_demand_multiplier = no_full_flex_tasks_min + no_semi_flex_tasks_min + no_fixed_tasks_min
care_f_max = 10
care_f_weight = 1

# pricing related parameters
pricing_table_weight = 1
# cost_function_type = "linear"
cost_function_type = "piece-wise"


# solver related parameters
variable_selection = "smallest"
value_choice = "indomain_min"
model_type = "pre"
solver_type = "cp"
solver_name = "gecode"

# external file related parameters
parent_folder = ""
file_cp_pre = parent_folder + 'data/Household-cp-pre.mzn'
file_cp_ini = parent_folder + 'data/Household-cp.mzn'
file_pricing_table = parent_folder + 'data/pricing_table_0.csv'
file_household_area_folder = parent_folder + 'data/'
file_pdp = parent_folder + 'data/probability.csv'
file_demand_list = parent_folder + 'data/demands_list.csv'
result_folder = parent_folder + "results/"
file_community_pkl = "community.pkl"
file_community_meta_pkl = "community_aggregate.pkl"
file_aggregator_pkl = "aggregator.pkl"
file_pricing_table_pkl = "pricing_table.pkl"

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

# demand related parameters
k_household_key = "key"
k_starts = "start_times"
k_demand = "demands"
k_demand_max = "max_demand"
k_demand_max_init = "init_max_demand"
k_demand_reduction = "demand_reduction"
k_demand_total = "total_demand"
k_par = "PAR"
k_par_init = "init_PAR"
k_final = "final"

# step size
k0_step = "step_size"

# objective related parameters
k0_cost = "cost"
k0_cost_reduction = "cost_reduction"
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
k1_minizinc = "minizinc"
k1_ogsa = "ogsa"
k2_before_fw = "scheduling"
k2_after_fw = "pricing"

# tracking-related
k0_tracker = "tracker"
k0_others = "others"
algorithms = dict()
algorithms[k1_minizinc] = dict()
algorithms[k1_minizinc][k2_before_fw] = k1_minizinc
algorithms[k1_minizinc][k2_after_fw] = f"{k1_minizinc}_fw"
algorithms[k1_ogsa] = dict()
algorithms[k1_ogsa][k2_before_fw] = k1_ogsa
algorithms[k1_ogsa][k2_after_fw] = f"{k1_ogsa}_fw"

algorithm_full_names = dict()
algorithm_full_names[algorithms[k1_minizinc][k2_before_fw]] = "MiniZinc model with data preprocessing"
algorithm_full_names[algorithms[k1_minizinc][k2_after_fw]] = "FW-DDSM with MiniZinc model and data preprocessing"
algorithm_full_names[algorithms[k1_ogsa][k2_before_fw]] = "OGSA"
algorithm_full_names[algorithms[k1_ogsa][k2_after_fw]] = "FW-DDSM with OGSA"




