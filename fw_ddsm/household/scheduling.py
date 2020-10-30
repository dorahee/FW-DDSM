from multiprocessing import Pool
from fw_ddsm.parameter import *
import timeit
import random as r



def minizinc_model(objective_values):
    return 0


def ogsa(objective_values, big_value, powers, durations, preferred_starts, latest_ends, max_demand,
         successors, precedents, succ_delays, num_intervals=no_intervals):
    start_time = timeit.default_timer()

    def retrieve_successors_or_precedents(list0, prec_or_succ_list1, succ_prec_list2):
        list_r = []
        for l in list0:
            if l in prec_or_succ_list1:
                succ_or_prec_indices = [i2 for i2, k in enumerate(prec_or_succ_list1) if k == l]
                succ_or_prec = [succ_prec_list2[i2] for i2 in succ_or_prec_indices]
                succ_succ_or_prec_prec \
                    = retrieve_successors_or_precedents(succ_or_prec, prec_or_succ_list1, succ_prec_list2)
                list_r.extend(succ_succ_or_prec_prec)
            else:
                list_r.append(l)
        return list_r

    def check_if_successors_or_precedents_exist(checked_task_id, prec_or_succ1, succ_or_prec2):
        succs_succs_or_precs_precs = []
        if checked_task_id in prec_or_succ1:
            indices = [i2 for i2, k in enumerate(prec_or_succ1) if k == checked_task_id]
            succs_or_precs = [succ_or_prec2[i2] for i2 in indices]
            succs_succs_or_precs_precs = retrieve_successors_or_precedents(succs_or_precs, prec_or_succ1, succ_or_prec2)

        return succs_succs_or_precs_precs

    actual_starts = []
    household_profile = [0] * num_intervals
    num_tasks = len(powers)
    for task_id in range(num_tasks):
        power = powers[task_id]
        duration = durations[task_id]
        task_costs = objective_values[task_id]

        # if i has successors
        tasks_successors = check_if_successors_or_precedents_exist(task_id, precedents, successors)
        earliest_suc_lstart_w_delay = 0
        earliest_suc_lstart = num_intervals - 1
        if bool(tasks_successors):
            suc_durations = [durations[i2] for i2 in tasks_successors]
            suc_lends = [latest_ends[i2] for i2 in tasks_successors]
            earliest_suc_lstart = min([lend - dur for lend, dur in zip(suc_lends, suc_durations)])
            # earliest_suc_lstart_w_delay = earliest_suc_lstart - succ_delay
            # suc_lstarts = [lend - dur + 1 for lend, dur in zip(suc_lends, suc_durations)]
            # earliest_suc_lstart = min(suc_lstarts)

        # if i has precedents
        tasks_precedents = check_if_successors_or_precedents_exist(task_id, successors, precedents)
        latest_pre_finish = 0
        latest_pre_finish_w_delay = num_intervals - 1
        if bool(tasks_precedents):
            prec_durations = [durations[i2] for i2 in tasks_precedents]
            prec_astarts = [actual_starts[i2] for i2 in tasks_precedents]
            succ_delay = succ_delays[task_id]
            latest_pre_finish = max([astart + dur - 1 for astart, dur in zip(prec_durations, prec_astarts)])
            latest_pre_finish_w_delay = latest_pre_finish + succ_delay[0]

        # search for all feasible intervals
        feasible_intervals = []
        for j in range(num_intervals):
            if task_costs[j] < big_value and earliest_suc_lstart_w_delay < j < earliest_suc_lstart - duration + 1 \
                    and latest_pre_finish < j < latest_pre_finish_w_delay:
                feasible_intervals.append(j)

        try:
            feasible_min_cost = min([task_costs[f] for f in feasible_intervals])
            cheapest_intervals = [f for f in feasible_intervals if task_costs[f] == feasible_min_cost]
            a_start = r.choice(cheapest_intervals)

            # check max demand constraint
            max_demand_starts = dict()
            temp_profile = household_profile[:]
            try:
                for d in range(a_start, a_start + duration):
                    temp_profile[d % num_intervals] += power
            except IndexError:
                print("error")
            temp_max_demand = max(temp_profile)
            while temp_max_demand > max_demand and len(feasible_intervals) > 1:

                max_demand_starts[a_start] = temp_max_demand
                feasible_intervals.remove(a_start)

                feasible_min_cost = min([objective_values[task_id][f] for f in feasible_intervals])
                feasible_min_cost_indices = [k for k, x in enumerate(objective_values[task_id]) if x == feasible_min_cost]
                # a_start = r.choice(feasible_min_cost_indices)
                a_start = feasible_min_cost_indices[0]

                temp_profile = household_profile[:]
                for d in range(a_start, a_start + duration):
                    temp_profile[d] += power
                temp_max_demand = max(temp_profile)

            if len(feasible_intervals) == 0 and not max_demand_starts:
                a_start = min(max_demand_starts, key=max_demand_starts.get)

        except ValueError:
            # print("No feasible intervals left for task", task_id)
            a_start = preferred_starts[task_id]

        actual_starts.append(a_start)

        # obj = 0
        for d in range(a_start, a_start + duration):
            household_profile[d % num_intervals] += power
        # obj += objective_values[task_id][a_start]

    time_scheduling_ogsa = timeit.default_timer() - start_time
    # obj = round(obj, 2)

    return actual_starts, household_profile, time_scheduling_ogsa


def schedule_household(household, prices, scheduling_method):

    def preprocessing():
        max_duration = max(durations)
        # this big cost and big cost * number_tasks need to be smaller than the largest number that the solver can handle
        big_value = max_demand * max_duration * max(prices) + \
                   inconvenience_cost_weight * max_care_factor * num_intervals
        objective_value_matrix = []
        for power, pst, est, lft, dur, cf in zip(powers, preferred_starts, earliest_starts,
                                                 latest_ends, durations, care_factors):
            objective_value_task = []
            for t in range(num_intervals):
                if est <= t <= lft - dur + 1:
                    rc = abs(t - pst) * cf * inconvenience_cost_weight
                    try:
                        rc += sum([prices[j % num_intervals] for j in range(t, t + dur)]) * power
                    except IndexError:
                        print("Error: check the prices.")
                else:
                    rc = big_value
                objective_value_task.append(int(rc))
            objective_value_matrix.append(objective_value_task)
        return objective_value_matrix, big_value

    key = household[h_key]
    powers = household[h_powers]
    durations = household[h_durs]
    earliest_starts = household[h_ests]
    latest_ends = household[h_lfs]
    preferred_starts = household[h_psts]
    care_factors = household[h_cfs]
    max_care_factor = household[h_max_cf]
    precedents = [x[0] for x in list(household[h_precs].values())]
    successors = list(household[h_precs].keys())
    succ_delays = household[h_succ_delay]  # need to change this format when sending it to the solver
    no_precedents = household[h_no_precs]
    max_demand = household[h_max_demand]
    inconvenience_cost_weight = household[h_incon_weight]
    num_intervals = len(household[h_demand_profile])
    num_periods = len(prices)
    num_intervals_period = int(num_intervals/num_periods)
    if num_periods != num_intervals:
        prices = [int(p) for p in prices for _ in range(num_intervals_period)]
    else:
        prices = [int(p) for p in prices]

    objective_values, big_value = preprocessing()
    if "minizinc" in scheduling_method:
        minizinc_model(objective_values)
    elif "ogsa" in scheduling_method:
        actual_starts, household_demand_profile, time_scheduling \
            = ogsa(objective_values, big_value, powers, durations, preferred_starts, latest_ends, max_demand,
         successors, precedents, succ_delays, num_intervals)
    else:
        actual_starts, household_demand_profile, time_scheduling \
            = ogsa(objective_values, big_value, powers, durations, preferred_starts, latest_ends, max_demand,
         successors, precedents, succ_delays, num_intervals)

    penalty_household = sum([abs(pst - ast) * cf for pst, ast, cf
                             in zip(preferred_starts, actual_starts, care_factors)]) * inconvenience_cost_weight

    return {h_key:key, k0_demand:household_demand_profile, k0_penalty:penalty_household,
            k0_starts:actual_starts, k0_time:time_scheduling}

def schedule_households(households, prices, num_iteration, scheduling_method):
    print("Start scheduling households...")
    pool = Pool()
    results = pool.starmap_async(schedule_household,
                                 [(household, prices, scheduling_method)
                                  for household in households.values()]).get()
    pool.close()
    pool.join()

    aggregate_demand_profile = [0] * no_intervals
    total_inconvenience_cost = 0
    time_scheduling_iteration = 0
    for res in results:
        key = res[h_key]
        households[key][k0_starts][scheduling_method][num_iteration] = res[k0_starts]
        households[key][k0_penalty][scheduling_method][num_iteration] = res[k0_penalty]
        households[key][k0_demand][scheduling_method][num_iteration] = res[k0_demand]

        aggregate_demand_profile = [x + y for x, y in zip(res[k0_demand], aggregate_demand_profile)]
        total_inconvenience_cost += res[k0_penalty]
        time_scheduling_iteration += res[k0_time]

    return households, aggregate_demand_profile, total_inconvenience_cost, time_scheduling_iteration
