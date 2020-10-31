from fw_ddsm.parameter import *
from fw_ddsm.aggregator.aggregator import *
from fw_ddsm.household.household import *
from fw_ddsm.community.community import *

class Experiment:

    def __init__(self, algorithms, num_households):
        self.algorithms = algorithms
        self.num_households = num_households
        self.num_iteration = 0


    def data_preparation(self, file_probability=file_probability,
                         file_demand_list=file_demand_list,
                         file_pricing_table=file_pricing_table):

        num_households = self.num_households

        # 1. generate new households, trackers and a pricing table
        self.households = Community(num_households=num_households)
        self.households.new(file_probability_path=file_probability,
                            file_demand_list_path=file_demand_list,
                            algorithms_options=self.algorithms)

        self.aggregator = Aggregator()
        self.aggregator.new(normalised_pricing_table_csv=file_pricing_table,
                     aggregate_preferred_demand_profile=self.households.preferred_demand_profile,
                     algorithms_options=self.algorithms)

        print("Households and the aggregator are ready. ")

    def iteration(self, algorithm):
        scheduling_method = algorithm[k2_before_fw]
        pricing_method = algorithm[k2_after_fw]

        # aggregator, k = 0
        aggregate_demand_profile = self.aggregator.data[pricing_method][k0_demand][0]
        prices, consumption_cost = self.aggregator.pricing(aggregate_demand_profile=aggregate_demand_profile)
        self.aggregator.update(num_iteration=0, pricing_method=pricing_method,
                               prices=prices, consumption_cost=consumption_cost)

        # update_aggregator(num_iteration=0, prices=prices, consumption_cost=consumption_cost)
        # step = 1
        #
        # num_iteration = 1
        # while step > 0:
        #
        #     # households, k > 0
        #     households, aggregate_demand_profile, total_inconvenience_cost, time_scheduling_iteration \
        #         = schedule_households(households=households, prices=prices, num_iteration=num_iteration,
        #                               scheduling_method=scheduling_method)
        #
        #
        #
        #     num_iteration += 1