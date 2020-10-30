from fw_ddsm import iteration
from household import generation
from aggregator import build
from fw_ddsm.parameter import *


def experiment(algorithms,
               file_probability=file_probability,
               file_demand_list=file_demand_list,
               file_pricing_table=file_pricing_table):

    # 1. generate new households, trackers and a pricing table
    households, aggregate_demand_profile \
        = generation.new_households(num_households=10, algorithms_options=algorithms, file_probability_path=file_probability, file_demand_list_path=file_demand_list)

    aggregator = build.new_aggregate(aggregate_preferred_demand_profile=aggregate_demand_profile,
                                          algorithms_options=algorithms)

    pricing_table = build.new_pricing_table(normalised_pricing_table_csv=file_pricing_table,
                                                 maximum_demand_level=aggregator[k0_demand_max])

    # 2. begin the iteration
    for alg in algorithms.values():
        iteration.start_iteration(households=households, aggregator=aggregator,
                                  pricing_table=pricing_table, algorithm=alg)
    print()
