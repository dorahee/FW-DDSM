from fw_ddsm.parameter import *
from fw_ddsm.aggregator.aggregator import *
from fw_ddsm.household.household import *
from fw_ddsm.community.community import *


class Experiment:

    def __init__(self, algorithms, num_households):
        self.algorithms = algorithms
        self.num_households = num_households
        self.num_iteration = 0

    def new_data(self, file_probability=file_probability,
                 file_demand_list=file_demand_list,
                 file_pricing_table=file_pricing_table,
                 data_folder="test2"):
        self.data_folder = data_folder

        # 1. generate new households, trackers and a pricing table
        self.community = Community()
        self.community.new(num_households=self.num_households,
                           file_probability_path=file_probability,
                           file_demand_list_path=file_demand_list,
                           algorithms_options=self.algorithms,
                           write_to_file_path=self.data_folder)

        self.aggregator = Aggregator()
        self.aggregator.new(normalised_pricing_table_csv=file_pricing_table,
                            aggregate_preferred_demand_profile=self.community.preferred_demand_profile,
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
        aggregate_demand_profile = self.aggregator.data[pricing_method][k0_demand][0]
        prices, consumption_cost = self.aggregator.prices_and_cost(aggregate_demand_profile=aggregate_demand_profile)
        self.aggregator.update(num_iteration=0, pricing_method=pricing_method,
                               step=step, demands=aggregate_demand_profile, prices=prices, consumption_cost=consumption_cost,
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
