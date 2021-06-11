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


def find_step_size(num_iteration, pricing_method, pricing_table,
                   aggregate_demand_profile_new, aggregate_demand_profile_fw_pre,
                   aggregate_battery_profile_new, aggregate_battery_profile_fw_pre,
                   total_inconvenience_new, total_inconvenience_fw_pre,
                   total_cost_fw_pre, price_fw_pre,
                   min_step_size=min_step, ignore_tiny_step=False, roundup_tiny_step=False, print_steps=False,
                   obj_par=False):
    time_begin = time()

    def move_profile(demands_pre, demands_new, alpha):
        return [d_p + (d_n - d_p) * alpha for d_p, d_n in zip(demands_pre, demands_new)]

    changes_of_aggregate_demand_profile \
        = [d_n - d_p for d_n, d_p in zip(aggregate_demand_profile_new, aggregate_demand_profile_fw_pre)]
    change_of_inconvenience = total_inconvenience_new - total_inconvenience_fw_pre
    PAR_fw_pre = max(aggregate_demand_profile_fw_pre) / average(aggregate_demand_profile_fw_pre)

    step_size_final = 0
    step_size_final_temp = 0
    step_size_final_temp_prev = -1
    num_itrs = -1
    change_of_obj = -999
    change_of_cost = -999
    min_abs_change = 0.0001

    # if the gradient is less than zero and the step size is not yet 1, continue the loop
    # ! test the step_size_final_temp for termination condition instead of step_size_final !
    while change_of_obj < 0 and abs(change_of_obj) > min_abs_change and step_size_final_temp < 1 \
            and not step_size_final_temp == step_size_final_temp_prev:

        step_size_final = step_size_final_temp
        num_itrs += 1

        # search for the step per time period
        step_profile = []
        for dp, dn, demand_levels_period in \
                zip(aggregate_demand_profile_fw_pre, aggregate_demand_profile_new,
                    pricing_table[p_demand_table].values()):
            d_levels = list(demand_levels_period.values())[:-1]
            min_demand_level = min(d_levels)
            max_demand_level = d_levels[-1]
            second_max_demand_level = d_levels[-2]
            if dn < dp < min_demand_level or dn > dp > second_max_demand_level:
                step = 0.001
            elif dn == dp or dp < dn < min_demand_level or dp > dn > max_demand_level:
                step = 1
            else:
                dd = dn - dp
                dl = find_ge(d_levels, dp) + 0.001 if dd > 0 else find_le(d_levels, dp) - 0.001
                step = (dl - dp) / dd
                if ignore_tiny_step:
                    step = step if step > min_step_size else 1
                if roundup_tiny_step:
                    step = ceil(step * 1000) / 1000
                step = max(step, min_step_size)
            step_profile.append(step)

        # find the smallest step size
        step_size_incr = min(step_profile)
        if step_size_incr == 1:
            print("step size incr is one")
        step_size_final_temp_prev = step_size_final_temp
        step_size_final_temp = step_size_final + step_size_incr

        # update the aggregate demand profile using the current step-size
        aggregate_demand_profile_fw_temp \
            = move_profile(aggregate_demand_profile_fw_pre, aggregate_demand_profile_new, step_size_final_temp)
        PAR_fw_temp = max(aggregate_demand_profile_fw_temp) / average(aggregate_demand_profile_fw_temp)

        # update the prices and the cost using the updated aggregated demand profile
        price_fw_temp, cost_fw_temp = prices_and_cost(aggregate_demand_profile=aggregate_demand_profile_fw_temp,
                                                      pricing_table=pricing_table,
                                                      cost_function=cost_function_type)

        # calculate the gradient/change of objective
        change_of_cost = sum([d_c * p_fw for d_c, p_fw in zip(changes_of_aggregate_demand_profile, price_fw_temp)])
        change_of_PAR = PAR_fw_temp - PAR_fw_pre
        change_of_obj = change_of_inconvenience + change_of_cost + change_of_PAR * int(obj_par)

        if print_steps:
            print(f"step {step_size_final_temp} at {num_itrs}, change of cost = {change_of_cost}, "
                  f"change of obj = {change_of_obj}")


    # update aggregate demand profile, aggregate battery profile, total cost, total inconvenience and total obj
    aggregate_demand_profile_fw \
        = move_profile(aggregate_demand_profile_fw_pre, aggregate_demand_profile_new, step_size_final)
    aggregate_battery_profile_fw \
        = move_profile(aggregate_battery_profile_fw_pre, aggregate_battery_profile_new, step_size_final)
    price_fw, total_cost_fw \
        = prices_and_cost(aggregate_demand_profile=aggregate_demand_profile_fw,
                          pricing_table=pricing_table,
                          cost_function=cost_function_type)
    total_inconvenience_fw = total_inconvenience_fw_pre + step_size_final * change_of_inconvenience
    PAR_fw = max(aggregate_demand_profile_fw) / average(aggregate_demand_profile_fw)
    total_obj_fw = total_cost_fw + total_inconvenience_fw + PAR_fw * int(obj_par)

    print(f"{num_iteration}. "
          f"Best step size {round(step_size_final, 6)}, "
          f"{num_itrs} iterations, "
          f"obj {total_obj_fw}, "
          f"change of obj {change_of_obj}, "
          f"using {pricing_method}")
    time_fw = time() - time_begin

    return aggregate_demand_profile_fw, aggregate_battery_profile_fw, \
           step_size_final, price_fw, total_cost_fw, total_inconvenience_fw, time_fw


def compute_start_time_probabilities(history_steps):
    prob_dist = []
    if history_steps[0] == 0 or history_steps[0] == 1:
        del history_steps[0]

    for alpha in history_steps:
        if not prob_dist:
            prob_dist.append(1 - alpha)
            prob_dist.append(alpha)
        else:
            prob_dist = [p_d * (1 - alpha) for p_d in prob_dist]
            prob_dist.append(alpha)

    return prob_dist
