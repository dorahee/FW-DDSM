from fw_ddsm.aggregator.aggregator import *
from fw_ddsm.community.community import *


class Experiment:

    def __init__(self, algorithms, num_households):
        self.algorithms = algorithms
        self.num_households = num_households
        self.num_iteration = 0
        self.num_intervals = 0

    def new_data(self, file_demand_list=file_demand_list,
                 file_pricing_table=file_pricing_table,
                 data_folder="test2",
                 file_probability=file_probability,
                 max_demand_multiplier=maxium_demand_multiplier,
                 num_tasks_dependent=no_tasks_dependent,
                 full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
                 semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
                 fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
                 inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max):
        self.data_folder = data_folder

        # 1. generate new households, trackers and a pricing table
        self.community = Community()
        self.community.new(num_households=self.num_households,
                           file_probability_path=file_probability,
                           file_demand_list_path=file_demand_list,
                           algorithms_options=self.algorithms,
                           write_to_file_path=self.data_folder,
                           max_demand_multiplier=max_demand_multiplier,
                           num_tasks_dependent=num_tasks_dependent,
                           full_flex_task_min=full_flex_task_min, full_flex_task_max=full_flex_task_max,
                           semi_flex_task_min=semi_flex_task_min, semi_flex_task_max=semi_flex_task_max,
                           fixed_task_min=fixed_task_min, fixed_task_max=fixed_task_max,
                           inconvenience_cost_weight=inconvenience_cost_weight, max_care_factor=max_care_factor)

        self.aggregator = Aggregator()
        self.aggregator.new(normalised_pricing_table_csv=file_pricing_table,
                            aggregate_preferred_demand_profile=self.community.aggregate_data[k0_demand],
                            algorithms_options=self.algorithms, write_to_file_path=self.data_folder)

        print("Households and the aggregator are created. ")

    def read_data(self, read_from_folder="test2"):
        self.community = Community()
        self.community.read(read_from_file=read_from_folder)
        self.aggregator = Aggregator()
        self.aggregator.read(read_from_file=read_from_folder)

        print("Households and the aggregator are read. ")

    def iteration(self, algorithm):
        scheduling_method = algorithm[k2_before_fw]
        pricing_method = algorithm[k2_after_fw]

        # aggregator, k = 0
        step = 1
        aggregate_demand_profile = self.aggregator.aggregator[pricing_method][k0_demand][0]
        prices, consumption_cost = self.aggregator.prices_and_cost(aggregate_demand_profile=aggregate_demand_profile)
        self.aggregator.update(num_iteration=0, pricing_method=pricing_method,
                               step=step, demands=aggregate_demand_profile, prices=prices,
                               consumption_cost=consumption_cost,
                               inconvenience_cost=0)

        num_iteration = 1
        while step > 0:
            # community, k > 0
            aggregate_demand_profile_new, total_inconvenience, time_scheduling_iteration \
                = self.community.schedule_all(num_iteration=num_iteration, prices=prices,
                                              scheduling_method=scheduling_method)
            self.community.update_aggregate_data(num_iteration=num_iteration, demands=aggregate_demand_profile_new,
                                                 penalty=total_inconvenience, time=time_scheduling_iteration,
                                                 scheduling_method=scheduling_method)

            # aggregator, k > 0
            aggregate_demand_profile_fw, step, prices, total_consumption_cost_fw, total_inconvenience_fw \
                = self.aggregator.find_step_size(num_iteration=num_iteration,
                                                 demand_profile=aggregate_demand_profile_new,
                                                 inconvenience=total_inconvenience,
                                                 pricing_method=pricing_method)
            self.aggregator.update(num_iteration=num_iteration, pricing_method=pricing_method,
                                   step=step, prices=prices, consumption_cost=total_consumption_cost_fw,
                                   demands=aggregate_demand_profile_fw, inconvenience_cost=total_inconvenience_fw)

            num_iteration += 1

        print(f"Converged in {num_iteration - 1}")
        #
        #
        #
        #     num_iteration += 1

    def final_schedule(self, algorithm):
        scheduling_method = algorithm[k2_before_fw]
        pricing_method = algorithm[k2_after_fw]

        start_time_probability_distribution = self.aggregator.compute_start_time_probabilities(pricing_method)
        final_aggregate_demand_profile, final_total_inconvenience \
            = self.community.decide_final_schedules(scheduling_method=scheduling_method,
                                                    probability_distribution=start_time_probability_distribution)

        final_prices, final_consumption_cost = self.aggregator.prices_and_cost(final_aggregate_demand_profile)
        self.aggregator.update(num_iteration=None, final=True, pricing_method=pricing_method,
                               demands=final_aggregate_demand_profile, prices=final_prices,
                               consumption_cost=final_consumption_cost, inconvenience_cost=final_total_inconvenience)

        print(self.aggregator.aggregator[pricing_method][k0_final][k0_demand])
        print(final_prices)
        print(f"Preferred cost is {self.aggregator.aggregator[pricing_method][k0_cost][0]}, "
              f"PAR is {self.aggregator.aggregator[pricing_method][k0_par][0]}")
        print(f"Final cost is {final_consumption_cost}, "
              f"PAR is {self.aggregator.aggregator[pricing_method][k0_final][k0_par]} and "
              f"inconvenience is {final_total_inconvenience}.")

        return final_aggregate_demand_profile, final_total_inconvenience
