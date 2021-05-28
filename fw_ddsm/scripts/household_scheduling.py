from minizinc import *
import timeit
import random as r
from datetime import timedelta
from fw_ddsm.parameter import *


def tasks_preprocessing(
        powers, durations, max_demand, prices, preferred_starts, earliest_starts, latest_ends, care_factors,
        inconvenience_cost_weight, max_care_factor, num_intervals=no_intervals
):
    num_intervals_hour = num_intervals / 24
    max_duration = max(durations)
    # this big cost and big cost * number_tasks need to be
    # smaller than the largest number that the solver can handle
    max_prices = min(max(prices), 300)
    big_value = max_demand * max_duration * max_prices + \
                inconvenience_cost_weight * max_care_factor * num_intervals
    objective_value_matrix = []
    for power, pst, est, lft, dur, cf in zip(powers, preferred_starts, earliest_starts,
                                             latest_ends, durations, care_factors):
        objective_value_task = []
        for t in range(num_intervals):
            if est <= t <= lft - dur + 1:
                rc = abs(t - pst) * cf * inconvenience_cost_weight
                try:
                    rc += sum([prices[j % num_intervals] for j in range(t, t + dur)]) * power / num_intervals_hour
                except IndexError:
                    print("Error: check the prices.")
            else:
                rc = big_value
            objective_value_task.append(int(rc))
        objective_value_matrix.append(objective_value_task)
    return objective_value_matrix, big_value


def tasks_minizinc(model_file, solver, search,
                   objective_values, powers, max_demand, durations,
                   earliest_starts, preferred_starts, latest_ends,
                   successors, precedents, no_precedents, succ_delays,
                   care_factors, prices, inconvenience_cost_weight,
                   num_intervals=no_intervals, timeout=time_out):
    # problem model
    model = Model(model_file)
    gecode = Solver.lookup(solver)
    model.add_string("solve ")
    if "gecode" in solver:
        model.add_string(":: {} ".format(search))
    model.add_string("minimize obj;")

    ins = Instance(gecode, model)
    num_tasks = len(powers)
    ins["num_intervals"] = num_intervals
    ins["num_tasks"] = num_tasks
    ins["durations"] = durations
    ins["demands"] = powers
    ins["num_precedences"] = no_precedents
    ins["predecessors"] = [p + 1 for p in precedents]
    ins["successors"] = [s + 1 for s in successors]
    ins["prec_delays"] = succ_delays
    ins["max_demand"] = max_demand

    if "ini" in tasks_model_type.lower():
        ins["prices"] = prices
        ins["preferred_starts"] = [ps + 1 for ps in preferred_starts]
        ins["earliest_starts"] = [es + 1 for es in earliest_starts]
        ins["latest_ends"] = [le + 1 for le in latest_ends]
        ins["care_factors"] = [cf * inconvenience_cost_weight for cf in care_factors]
    else:
        ins["run_costs"] = objective_values

    # solve problem model
    if timeout is None:
        result = ins.solve()
    else:
        result = ins.solve(timeout=timedelta(seconds=timeout))

    # process problem solution
    # obj = result.objective
    # print(result)
    solution = result.solution.actual_starts
    actual_starts = [int(a) - 1 for a in solution]
    # if "cp" in solver_type:
    #     actual_starts = [int(a) - 1 for a in solution]
    # else:  # "mip" in solver_type:
    #     actual_starts = [sum([i * int(v) for i, v in enumerate(row)]) for row in solution]
    time = result.statistics["time"].total_seconds()

    return actual_starts, time


def tasks_ogsa(objective_values, big_value, powers, durations, preferred_starts, latest_ends, max_demand,
               successors, precedents, succ_delays, randomness=True, num_intervals=no_intervals):
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
                list_r.append(int(l))
        return list_r

    def check_if_successors_or_precedents_exist(checked_task_id, prec_or_succ1, succ_or_prec2):
        succs_succs_or_precs_precs = []
        if checked_task_id in prec_or_succ1:
            indices = [i2 for i2, k in enumerate(prec_or_succ1) if k == checked_task_id]
            succs_or_precs = [succ_or_prec2[i2] for i2 in indices]
            succs_succs_or_precs_precs = retrieve_successors_or_precedents(succs_or_precs, prec_or_succ1,
                                                                           succ_or_prec2)
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
            s = earliest_suc_lstart_w_delay < j < earliest_suc_lstart - duration + 1 if bool(tasks_successors) else True
            p = latest_pre_finish < j < latest_pre_finish_w_delay if bool(tasks_precedents) else True
            if task_costs[j] < big_value and s and p:
                feasible_intervals.append(j)

        try:
            feasible_min_cost = min([task_costs[f] for f in feasible_intervals])
            cheapest_intervals = [f for f in feasible_intervals if task_costs[f] == feasible_min_cost]
            a_start = r.choice(cheapest_intervals) if randomness else cheapest_intervals[0]

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
                feasible_min_cost_indices = [k for k, x in enumerate(objective_values[task_id]) if
                                             x == feasible_min_cost]
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

        # obj += objective_values[task_id][a_start]

    time_scheduling_ogsa = timeit.default_timer() - start_time
    # obj = round(obj, 2)

    return actual_starts, time_scheduling_ogsa


def battery_mip(model_file, solver, existing_demands, capacity_max, capacity_min, power_max, prices,
                fully_charge_time=fully_charge_hour, num_intervals=no_intervals, timeout=time_out):

    def rotate_list(l_original, p):
        if p == 0:
            return l_original
        else:
            return l_original[p:] + l_original[:p]

    model = Model(model_file)
    mip_solver = Solver.lookup(solver)
    # model.add_string("solve minimize obj;")
    ins = Instance(mip_solver, model)

    # time parameters
    num_intervals_hour = int(num_intervals / 24)
    ins["num_intervals"] = num_intervals
    ins["num_intervals_hour"] = num_intervals_hour

    # battery parameters
    ins["max_energy_capacity"] = capacity_max
    ins["min_energy_capacity"] = capacity_min
    ins["max_power"] = power_max
    ins["fully_charge_hour"] = fully_charge_time

    # demands and prices
    # manipulate existing demands to start from the fully charged time
    fully_charged_intervals = fully_charge_time * num_intervals_hour
    existing_demands2 = rotate_list(existing_demands, fully_charged_intervals)
    prices2 = rotate_list(prices, fully_charged_intervals)
    ins["existing_demands"] = existing_demands2
    ins["prices"] = prices2

    if timeout is None:
        result = ins.solve()
    else:
        result = ins.solve(timeout=timedelta(seconds=timeout))

    battery_profile2 = result.solution.battery_profile
    # recover a battery profile that starts from 12am
    battery_profile = rotate_list(battery_profile2, -fully_charged_intervals)
    total_demand2 = result.solution.battery_profile
    time = result.statistics["time"].total_seconds()

    return battery_profile, time
