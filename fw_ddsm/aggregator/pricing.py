from fw_ddsm.cfunctions import *
from fw_ddsm.parameter import *


def pricing(aggregate_demand_profile, pricing_table, cost_function="piece-wise"):
    prices = []
    consumption_cost = 0

    price_levels = pricing_table[k0_price_levels]
    for demand_period, demand_level_period in \
            zip(aggregate_demand_profile, pricing_table[k0_demand_table].values()):
        demand_level = list(demand_level_period.values())
        level = bisect_left(demand_level, demand_period)
        if level != len(demand_level):
            price = price_levels[level]
        else:
            price = price_levels[-1]
        prices.append(price)

        if "piece-wise" in cost_function and level > 0:
            consumption_cost += demand_level[0] * price_levels[0]
            consumption_cost += (demand_period - demand_level[level - 1]) * price
            consumption_cost += sum([(demand_level[i] - demand_level[i - 1]) *
                                     price_levels[i] for i in range(1, level)])
        else:
            consumption_cost += demand_period * price

    consumption_cost = round(consumption_cost, 2)

    return prices, consumption_cost




