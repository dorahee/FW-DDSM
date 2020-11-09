from math import ceil
from time import time
from fw_ddsm.parameter import *
from fw_ddsm.scripts.custom_functions import *


def prices_and_cost(aggregate_demand_profile, pricing_table, cost_function=cost_function_type):
    prices = []
    consumption_cost = 0

    price_levels = pricing_table[p_price_levels]
    for demand_period, demand_level_period in \
            zip(aggregate_demand_profile, pricing_table[p_demand_table].values()):
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


def find_step_size(num_iteration, pricing_method, pricing_table, aggregate_demand_profile, aggregate_inconvenience,
                   demand_profile_fw_pre, inconvenience_fw_pre, price_fw_pre, cost_fw_pre):
    time_begin = time()

    price_fw = price_fw_pre[:]
    cost_fw = cost_fw_pre
    demand_profile_fw = demand_profile_fw_pre[:]
    inconvenience_fw = inconvenience_fw_pre
    change_of_inconvenience = aggregate_inconvenience - inconvenience_fw_pre
    demand_profile_changed = [d_n - d_p for d_n, d_p in zip(aggregate_demand_profile, demand_profile_fw_pre)]

    step_size_final = 0
    min_step_size = 0.001
    gradient = -999
    num_itrs = 0
    while gradient < 0 and step_size_final < 1:
        step_profile = []
        for dp, dn, demand_levels_period in \
                zip(demand_profile_fw_pre, aggregate_demand_profile, pricing_table[p_demand_table].values()):
            d_levels = list(demand_levels_period.values())[:-1]
            min_demand_level = min(d_levels)
            max_demand_level = d_levels[-1]
            second_max_demand_level = d_levels[-2]
            if dn < dp < min_demand_level or dp < dn < min_demand_level or dn > dp > second_max_demand_level \
                    or dp > dn > max_demand_level or dn == dp:
                step = 1
            else:
                dd = dn - dp
                dl = find_ge(d_levels, dp) + 0.01 if dd > 0 else find_le(d_levels, dp) - 0.01
                step = (dl - dp) / dd
                step = ceil(step * 1000) / 1000
                # step = step if step > min_step_size else 1
                step = max(step, min_step_size)
            step_profile.append(step)
        step_size_incr = min(step_profile)

        demand_profile_fw_temp = [d_p + (d_n - d_p) * step_size_incr for d_p, d_n in
                                  zip(demand_profile_fw_pre, aggregate_demand_profile)]
        price_fw_temp, cost_fw_temp = prices_and_cost(aggregate_demand_profile=demand_profile_fw_temp,
                                                      pricing_table=pricing_table,
                                                      cost_function=cost_function_type)
        gradient = change_of_inconvenience \
                   + sum([d_c * p_fw for d_c, p_fw in zip(demand_profile_changed, price_fw_temp)])

        demand_profile_fw_pre = demand_profile_fw_temp[:]
        step_size_final_temp = step_size_final + step_size_incr
        if gradient < 0 and step_size_final_temp < 1:
            step_size_final = step_size_final_temp
            demand_profile_fw = demand_profile_fw_temp[:]
            price_fw = price_fw_temp[:]
            cost_fw = cost_fw_temp
            inconvenience_fw = inconvenience_fw_pre + step_size_final * change_of_inconvenience
            num_itrs += 1

    print(f"{num_iteration}. "
          f"Best step size {round(step_size_final, 3)}, "
          f"{num_itrs} iterations, cost {cost_fw}, "
          f"using {pricing_method}")
    time_fw = time() - time_begin
    return demand_profile_fw, step_size_final, price_fw, cost_fw, inconvenience_fw, time_fw


def compute_start_time_probabilities(history_steps):
        prob_dist = []
        if history_steps[0] == 0 or history_steps[0] ==1:
            del history_steps[0]

        for alpha in history_steps:
            if not prob_dist:
                prob_dist.append(1 - alpha)
                prob_dist.append(alpha)
            else:
                prob_dist = [p_d * (1 - alpha) for p_d in prob_dist]
                prob_dist.append(alpha)

        return prob_dist
