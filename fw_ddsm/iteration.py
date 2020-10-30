from aggregator.pricing import *
from household.scheduling import *


def start_iteration(households, aggregator, pricing_table, algorithm):
    scheduling_method = algorithm[k2_before_fw]
    pricing_method = algorithm[k2_after_fw]

    def update_aggregator(num_iteration, step=None,
                                   prices=None, consumption_cost=None,
                                   demands=None, inconvenience_cost=None):
        if step is not None:
            aggregator[pricing_method][k0_step][num_iteration] = step
        if prices is not None:
            aggregator[pricing_method][k0_prices][num_iteration] = prices
        if consumption_cost is not None:
            aggregator[pricing_method][k0_cost][num_iteration] = consumption_cost
        if demands is not None:
            aggregator[pricing_method][k0_demand][num_iteration] = demands
            aggregator[pricing_method][k0_demand_max][num_iteration] = max(demands)
            aggregator[pricing_method][k0_demand_total][num_iteration] = sum(demands)
            aggregator[pricing_method][k0_par][num_iteration] = max(demands) / average(demands)
        if inconvenience_cost is not None:
            aggregator[pricing_method][k0_penalty][num_iteration] = inconvenience_cost

    # aggregator, k = 0
    aggregate_demand_profile = aggregator[pricing_method][k0_demand][0]
    prices, consumption_cost = pricing(aggregate_demand_profile=aggregate_demand_profile,
                                       pricing_table=pricing_table)
    update_aggregator(num_iteration=0, prices=prices, consumption_cost=consumption_cost)
    step = 1

    num_iteration = 1
    while step > 0:

        # households, k > 0
        households, aggregate_demand_profile, total_inconvenience_cost, time_scheduling_iteration \
            = schedule_households(households=households, prices=prices,
                                  num_iteration=num_iteration,
                                  scheduling_method=scheduling_method)



        num_iteration += 1










