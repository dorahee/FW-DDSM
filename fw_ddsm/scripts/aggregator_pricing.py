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
                   total_obj_new, price_fw_pre, total_cost_fw_pre,
                   min_step_size=min_step, roundup_tiny_step=False, print_steps=False,
                   obj_par=False):
    def move_profile(demands_pre, demands_new, alpha):
        return [d_p + (d_n - d_p) * alpha for d_p, d_n in zip(demands_pre, demands_new)]

    def find_smallest_step_increment(demands_temp, demands_new):
        step_profile = []
        for dp, dn, demand_levels_period in \
                zip(demands_temp, demands_new, pricing_table[p_demand_table].values()):
            d_levels = list(demand_levels_period.values())[:-1]
            min_demand_level = min(d_levels)
            max_demand_level = d_levels[-1]
            second_max_demand_level = d_levels[-2]
            if dn < dp < min_demand_level or dn > dp > second_max_demand_level:
                step = 0.001 if obj_par else 1
            elif dn == dp or dp < dn < min_demand_level or dp > dn > max_demand_level:
                step = 1
            else:
                dd = dn - dp
                dl = find_ge(d_levels, dp) + 0.01 if dd > 0 else find_le(d_levels, dp) - 0.01
                step = (dl - dp) / dd
                if roundup_tiny_step:
                    step = ceil(step * 10000) / 10000
                step = max(step, min_step_size)
            step_profile.append(step)

        step_size_incr = min(step_profile)

        return step_size_incr

    # start the timer
    time_begin = time()

    # by default, the FW outputs are the same as the inputs
    total_obj_fw_pre = total_cost_fw_pre + total_inconvenience_fw_pre
    step_size_fw = 0
    aggregate_demand_profile_fw = aggregate_demand_profile_fw_pre[:]
    price_fw = price_fw_pre[:]
    total_cost_fw = total_cost_fw_pre
    total_inconvenience_fw = total_inconvenience_fw_pre
    total_obj_fw = total_obj_fw_pre
    change_of_inconvenience = total_inconvenience_new - total_inconvenience_fw_pre
    change_of_obj_fw = total_obj_new - total_obj_fw_pre

    # initialise temporary variables for the FW iteration
    step_size_fw_temp = 0.0001
    aggregate_demand_profile_fw_temp = aggregate_demand_profile_fw_pre[:]
    price_fw_temp = price_fw_pre[:]
    total_cost_fw_temp = total_cost_fw_pre
    total_inconvenience_fw_temp = total_inconvenience_fw_pre
    total_obj_fw_temp = total_obj_fw_pre
    change_of_obj_temp = -999

    # set counters for the FW iteration
    num_itrs = -1
    while change_of_obj_temp <= 0 < step_size_fw_temp <= 1:

        # start the counter
        num_itrs += 1

        # update the FW outcomes from the second iteration
        if num_itrs > 0:
            step_size_fw = step_size_fw_temp
            aggregate_demand_profile_fw = aggregate_demand_profile_fw_temp
            price_fw = price_fw_temp
            total_cost_fw = total_cost_fw_temp
            total_inconvenience_fw = total_inconvenience_fw_temp
            total_obj_fw = total_obj_fw_temp
            change_of_obj_fw = total_obj_fw - total_obj_fw_pre

        if change_of_obj_temp == 0:
            break

        # search for the smallest step size from all time periods
        step_size_incr = find_smallest_step_increment(aggregate_demand_profile_fw_temp, aggregate_demand_profile_new)

        # update the temporary FW step size
        step_size_fw_temp = step_size_fw + step_size_incr

        # update the temporary aggregate demand profile using the current step-size
        aggregate_demand_profile_fw_temp \
            = move_profile(aggregate_demand_profile_fw_pre, aggregate_demand_profile_new, step_size_fw_temp)

        # update the temporary PAR
        PAR_fw_temp = max(aggregate_demand_profile_fw_temp) / average(aggregate_demand_profile_fw_temp)

        # update the temporary prices and total cost using the updated aggregated demand profile
        price_fw_temp, total_cost_fw_temp = prices_and_cost(aggregate_demand_profile=aggregate_demand_profile_fw_temp,
                                                            pricing_table=pricing_table,
                                                            cost_function=cost_function_type)

        # update the temporary total inconvenience
        total_inconvenience_fw_temp = total_inconvenience_fw_pre + step_size_fw_temp * change_of_inconvenience

        # update the temporary total objective
        total_obj_fw_temp = total_cost_fw_temp + total_inconvenience_fw_temp

        # update the temporary change of obj
        change_of_obj_temp = total_obj_fw_temp - total_obj_fw_pre

        # print intermediate results for debugging purpose
        if print_steps:
            print(f"step {step_size_fw_temp} at {num_itrs}, change of obj = {change_of_obj_temp}")

    # if total_obj_fw > total_obj_new and step_size_fw > 0:
    #     print("New solution is better.")
    #     step_size_fw = 1
    #     aggregate_demand_profile_fw = aggregate_demand_profile_new[:]
    #     price_fw, total_cost_fw = prices_and_cost(aggregate_demand_profile=aggregate_demand_profile_fw,
    #                                               pricing_table=pricing_table,
    #                                               cost_function=cost_function_type)
    #     total_inconvenience_fw = total_inconvenience_new
    #     total_obj_fw = total_cost_fw + total_inconvenience_fw

    # update the final FW battery profile
    aggregate_battery_profile_fw \
        = move_profile(aggregate_battery_profile_fw_pre, aggregate_battery_profile_new, step_size_fw)

    # print the FW outputs for debugging purpose
    print(f"{num_iteration}. "
          f"Step size = {round(step_size_fw, 6)}, "
          f"{num_itrs} iterations, "
          f"obj {round(total_obj_fw, 3)}, "
          f"incon {round(total_inconvenience_fw, 2)}, "
          f"change of obj {round(change_of_obj_fw, 3)}, "
          f"using {pricing_method}")

    # stop the timer
    time_fw = time() - time_begin

    return aggregate_demand_profile_fw, aggregate_battery_profile_fw, \
           step_size_fw, price_fw, total_cost_fw, total_inconvenience_fw, time_fw


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
