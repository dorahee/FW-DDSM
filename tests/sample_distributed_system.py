from aggregator import *
from household import *

algorithm = algorithms[m_ogsa]
scheduling_method = algorithm[m_before_fw]
pricing_method = algorithm[m_after_fw]

# ------------------------------ iteration = 0 ------------------------------
num_full_flex_task_min = 10
num_semi_flex_task_min = 15
num_fixed_task_min = 1
num_tasks_dependent = int((num_full_flex_task_min + num_semi_flex_task_min) / 5)
# household 1
household1 = Household()
household1.new(num_intervals=no_intervals,  ## change this to 48
               tasks_scheduling_method=scheduling_method,
               full_flex_task_min=num_full_flex_task_min,  ## change this to any number you want like 1, or 5
               semi_flex_task_min=num_semi_flex_task_min,  ## set it to 0
               fixed_task_min=num_fixed_task_min,  ## set it to 0
               num_tasks_dependent=num_tasks_dependent,
               file_preferred_demand_profile="data/sample_demand_profile.csv",
               file_list_of_devices_power="data/demands_list.csv",
               write_to_folder="households/", household_id=1)
# household1.read_household(scheduling_method=scheduling_method,
#                           read_from_folder="households/",
#                           household_id=1)
h1_demand_profile = household1.data_dict[s_demand]

# household 2
household2 = Household()
household2.new(num_intervals=no_intervals,
               tasks_scheduling_method=scheduling_method,
               full_flex_task_min=num_full_flex_task_min,
               semi_flex_task_min=num_semi_flex_task_min,
               fixed_task_min=num_fixed_task_min,
               num_tasks_dependent=num_tasks_dependent,
               file_preferred_demand_profile="data/sample_demand_profile.csv",
               file_list_of_devices_power="data/demands_list.csv",
               write_to_folder="households/", household_id=2)
# household2.read_household(scheduling_method=scheduling_method,
#                           read_from_folder="households/",
#                           household_id=2)
h2_demand_profile = household2.data_dict[s_demand]

# household 3
household3 = Household()
household3.new(num_intervals=no_intervals,
               tasks_scheduling_method=scheduling_method,
               full_flex_task_min=num_full_flex_task_min,
               semi_flex_task_min=num_semi_flex_task_min,
               fixed_task_min=num_fixed_task_min,
               num_tasks_dependent=num_tasks_dependent,
               file_preferred_demand_profile="data/sample_demand_profile.csv",
               file_list_of_devices_power="data/demands_list.csv",
               write_to_folder="households/", household_id=3)
# household3.read_household(scheduling_method=scheduling_method,
#                           read_from_folder="households/",
#                           household_id=3)
h3_demand_profile = household3.data_dict[s_demand]

# aggregator
aggregate_demand_profile_interval = [0] * no_intervals
aggregate_demand_profile_interval = [x + y for x, y in zip(h1_demand_profile, aggregate_demand_profile_interval)]
aggregate_demand_profile_interval = [x + y for x, y in zip(h2_demand_profile, aggregate_demand_profile_interval)]
aggregate_demand_profile_interval = [x + y for x, y in zip(h3_demand_profile, aggregate_demand_profile_interval)]

aggregator1 = Aggregator(num_periods=no_periods)
new_prices, consumption_cost \
    = aggregator1.new_aggregator(normalised_pricing_table_csv="data/pricing_table_0.csv",
                                 aggregate_preferred_demand_profile=aggregate_demand_profile_interval,
                                 pricing_method=pricing_method)
new_prices, consumption_cost \
    = aggregator1.read_aggregator(pricing_method=pricing_method,
                                  aggregate_preferred_demand_profile=aggregate_demand_profile_interval)

# ------------------------------ iteration > 0 ------------------------------#
num_iteration = 1

# household 1
h1_demand_profile, h1_penalty_weighted = household1.schedule(num_iteration=num_iteration, prices=new_prices)
# household 2
h2_demand_profile, h2_penalty_weighted = household2.schedule(num_iteration=num_iteration, prices=new_prices)
# household 3
h3_demand_profile, h3_penalty_weighted = household3.schedule(num_iteration=num_iteration, prices=new_prices)

# aggregator
aggregate_demand_profile = [0] * no_intervals
aggregate_demand_profile = [x + y for x, y in zip(h1_demand_profile, aggregate_demand_profile)]
aggregate_demand_profile = [x + y for x, y in zip(h2_demand_profile, aggregate_demand_profile)]
aggregate_demand_profile = [x + y for x, y in zip(h3_demand_profile, aggregate_demand_profile)]
aggregate_inconvenience = h1_penalty_weighted + h2_penalty_weighted + h3_penalty_weighted
new_prices, initial_consumption_cost, inconvenience, step, new_aggregate_demand_profile, time_pricing \
    = aggregator1.pricing(num_iteration=num_iteration,
                          aggregate_demand_profile=aggregate_demand_profile,
                          aggregate_inconvenience=aggregate_inconvenience)
initial_aggregate_demand_profile = aggregator1.preferred_demand_profile
initial_peak_demand = max(initial_aggregate_demand_profile)

while step > 0:
    num_iteration += 1
    print(f"{num_iteration}. Start scheduling...")
    # household 1
    h1_demand_profile, h1_penalty_weighted = household1.schedule(num_iteration=num_iteration, prices=new_prices)
    # household 2
    h2_demand_profile, h2_penalty_weighted = household2.schedule(num_iteration=num_iteration, prices=new_prices)
    # household 3
    h3_demand_profile, h3_penalty_weighted = household3.schedule(num_iteration=num_iteration, prices=new_prices)

    # aggregator
    aggregate_demand_profile = [0] * no_intervals
    aggregate_demand_profile = [x + y for x, y in zip(h1_demand_profile, aggregate_demand_profile)]
    aggregate_demand_profile = [x + y for x, y in zip(h2_demand_profile, aggregate_demand_profile)]
    aggregate_demand_profile = [x + y for x, y in zip(h3_demand_profile, aggregate_demand_profile)]
    aggregate_inconvenience = h1_penalty_weighted + h2_penalty_weighted + h3_penalty_weighted
    new_prices, consumption_cost, inconvenience, step, new_aggregate_demand_profile, time_pricing \
        = aggregator1.pricing(num_iteration=num_iteration,
                              aggregate_demand_profile=aggregate_demand_profile,
                              aggregate_inconvenience=aggregate_inconvenience)

print("------------------------------")
print("Iteration is finished. ")

# ------------------------------ finalising schedules ------------------------------#
print(f"Converged in {num_iteration}")

# aggregator
start_time_probability = aggregator1.compute_start_time_probabilities()

print(f"Initial: peak demand is {initial_peak_demand}, "
      f"total consumption is {sum(initial_aggregate_demand_profile)}, "
      f"consumption cost is {initial_consumption_cost}, "
      f"inconvenience cost is {0}. ")

for num_sample in range(5):
    # household 1
    h1_final_demand_profile, h1_final_inconvenience = household1.finalise_household(start_time_probability)

    # household 2
    h2_final_demand_profile, h2_final_inconvenience = household2.finalise_household(start_time_probability)

    # household 3
    h3_final_demand_profile, h3_final_inconvenience = household3.finalise_household(start_time_probability)

    # aggregator
    aggregate_final_demand_profile = [0] * no_intervals
    aggregate_final_demand_profile = [x + y for x, y in zip(h1_final_demand_profile, aggregate_final_demand_profile)]
    aggregate_final_demand_profile = [x + y for x, y in zip(h2_final_demand_profile, aggregate_final_demand_profile)]
    aggregate_final_demand_profile = [x + y for x, y in zip(h3_final_demand_profile, aggregate_final_demand_profile)]
    aggregate_inconvenience_final = h1_final_inconvenience + h2_final_inconvenience + h3_final_inconvenience

    prices, final_consumption_cost, final_inconvenience, step, final_aggregate_demand_profile, time_pricing \
        = aggregator1.pricing(num_iteration=0, aggregate_demand_profile=aggregate_final_demand_profile,
                              finalising=True, aggregate_inconvenience=aggregate_inconvenience_final)
    final_peak_demand = max(final_aggregate_demand_profile)

    print(f"{num_sample}. Final  : peak demand is {final_peak_demand} "
          f"({round((initial_peak_demand - final_peak_demand) / initial_peak_demand * 100)}% reduction), "
          f"total consumption is {sum(final_aggregate_demand_profile)}, "
          f"consumption cost is {final_consumption_cost}, "
          f"inconvenience cost is {aggregate_inconvenience}. ")
