from fw_ddsm.community import *
from fw_ddsm.aggregator import *


class Iteration:

    def __init__(self):
        self.scheduling_method = ""
        self.pricing_method = ""
        self.num_households = 0
        self.num_iteration = 0
        self.num_intervals = 0

        self.community = Community()
        self.aggregator = Aggregator()
        self.data_folder = "data/"
        self.start_time_probability = [1] * no_periods

    def new(self, algorithm, num_households,
            file_task_power=file_demand_list, max_demand_multiplier=maximum_demand_multiplier,
            file_normalised_pricing_table=file_pricing_table, file_preferred_demand_profile=file_pdp,
            num_tasks_dependent=no_tasks_dependent, ensure_dependent=False,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            data_folder=None, date_time=None):

        if data_folder is not None:
            if not data_folder.endswith("/"):
                data_folder += "/"
            self.data_folder = data_folder
        else:
            data_folder = self.data_folder

        self.scheduling_method = algorithm[m_before_fw]
        self.pricing_method = algorithm[m_after_fw]
        self.num_households = num_households

        # 1. generate new households, trackers and a pricing table
        preferred_demand_profile = self.community.new(
            num_households=self.num_households, scheduling_method=self.scheduling_method,
            file_preferred_demand_profile=file_preferred_demand_profile, file_demand_list=file_task_power,
            num_tasks_dependent=num_tasks_dependent, ensure_dependent=ensure_dependent,
            max_demand_multiplier=max_demand_multiplier,
            full_flex_task_min=full_flex_task_min, full_flex_task_max=full_flex_task_max,
            semi_flex_task_min=semi_flex_task_min, semi_flex_task_max=semi_flex_task_max,
            fixed_task_min=fixed_task_min, fixed_task_max=fixed_task_max,
            inconvenience_cost_weight=inconvenience_cost_weight, max_care_factor=max_care_factor,
            write_to_file_path=data_folder, date_time=date_time)
        prices, preferred_cost = self.aggregator.new_aggregator(
            normalised_pricing_table_csv=file_normalised_pricing_table,
            aggregate_preferred_demand_profile=preferred_demand_profile,
            pricing_method=self.pricing_method, write_to_file_path=data_folder, date_time=date_time)

        return preferred_demand_profile, prices

    def read(self, algorithm, inconvenience_cost_weight=None, new_dependent_tasks=None, ensure_dependent=False,
             read_from_folder="data/", date_time=None):

        if read_from_folder is None:
            read_from_folder = self.data_folder

        self.scheduling_method = algorithm[m_before_fw]
        self.pricing_method = algorithm[m_after_fw]
        preferred_demand_profile = self.community.read(
            read_from_folder=read_from_folder, scheduling_method=self.scheduling_method,
            inconvenience_cost_weight=inconvenience_cost_weight,
            num_dependent_tasks=new_dependent_tasks, ensure_dependent=ensure_dependent,
            date_time=date_time)
        prices, preferred_cost = self.aggregator.read_aggregator(
            read_from_folder=read_from_folder, date_time=date_time,
            pricing_method=self.pricing_method,
            aggregate_preferred_demand_profile=preferred_demand_profile)

        return preferred_demand_profile, prices

    def begin_iteration(self, starting_prices, num_cpus=None, timeout=time_out,
                        min_step_size=min_step, ignore_tiny_step=False, roundup_tiny_step=False,
                        print_done=False, print_steps=False):
        scheduling_method = self.scheduling_method
        pricing_method = self.pricing_method
        prices = starting_prices

        num_iteration = 1
        step = 1
        while step > 0:
            aggregate_demand_profile, weighted_total_inconvenience, time_scheduling_iteration \
                = self.community.schedule(num_iteration=num_iteration, prices=prices,
                                          scheduling_method=scheduling_method, num_cpus=num_cpus, timeout=timeout,
                                          print_done=print_done)
            prices, consumption_cost, inconvenience, step, new_aggregate_demand_profile, time_pricing \
                = self.aggregator.pricing(num_iteration=num_iteration,
                                          aggregate_demand_profile=aggregate_demand_profile,
                                          aggregate_inconvenience=weighted_total_inconvenience,
                                          min_step_size=min_step_size, ignore_tiny_step=ignore_tiny_step,
                                          roundup_tiny_step=roundup_tiny_step, print_steps=print_steps)
            num_iteration += 1

        print(f"Converged in {num_iteration - 1}")

        self.start_time_probability = self.aggregator.compute_start_time_probabilities()
        return self.start_time_probability, num_iteration - 1

    def finalise_schedules(self, start_time_probability=None, scheduling_method=None, num_samples=1):
        if scheduling_method is None:
            scheduling_method = self.scheduling_method
        if start_time_probability is None:
            start_time_probability = self.start_time_probability
        for i in range(1, num_samples + 1):
            final_aggregate_demand_profile, final_total_inconvenience \
                = self.community.finalise_schedule(num_sample=i,
                                                   scheduling_method=scheduling_method,
                                                   start_probability_distribution=start_time_probability)
            prices, consumption_cost, inconvenience, step, new_aggregate_demand_profile, time_pricing \
                = self.aggregator.pricing(num_iteration=i, aggregate_demand_profile=final_aggregate_demand_profile,
                                          finalising=True)
        # return consumption_cost, inconvenience
