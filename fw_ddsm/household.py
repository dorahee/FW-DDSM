import random as r
import numpy as np
from numpy import sqrt, pi, random
from numpy.random import choice
from json import dumps, load
from pathlib import Path
import timeit
from datetime import timedelta
from minizinc import *
from numpy import genfromtxt
from fw_ddsm.tracker import *


class Household:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)
        self.scheduling_method = ""
        self.tasks = dict()
        self.household_tracker = Tracker()
        self.household_final = Tracker()


    def read_household(self, scheduling_method, household_tracker=Tracker(), read_from_file=None):
            tasks = self.__existing_household(household_file=read_from_file)
            household_tracker.new(method=scheduling_method)
            household_tracker.update(num_record=0, method=scheduling_method, demands=self.tasks[k0_demand], penalty=0)
            print(f"Household{tasks[h_key]} is read.")
            return tasks, household_tracker


    def new_household(self, num_intervals, scheduling_method,
                      household_tracker=Tracker(),
                      preferred_demand_profile=None, list_of_devices_power=None,
                      preferred_demand_profile_csv=None, list_of_devices_power_csv=None,
                      max_demand_multiplier=maxium_demand_multiplier,
                      num_tasks_dependent=no_tasks_dependent,
                      full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
                      semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
                      fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
                      inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
                      write_to_file_path=None, household_id=0):
        if preferred_demand_profile is None and preferred_demand_profile_csv is None:
            print("Please provide a preferred demand profile or the csv. ")
        if list_of_devices_power is None and list_of_devices_power_csv is None:
            print("Please provide the power rates of the tasks. ")

        if preferred_demand_profile_csv is not None:
            preferred_demand_profile = genfromtxt(preferred_demand_profile_csv, delimiter=',', dtype="float")
        if list_of_devices_power_csv is not None:
            list_of_devices_power = genfromtxt(list_of_devices_power_csv, delimiter=',', dtype="float")

        tasks, household_demand_profile \
            = self.new_tasks(num_intervals=num_intervals,
                        preferred_demand_profile=preferred_demand_profile,
                        list_of_devices_power=list_of_devices_power,
                        max_demand_multiplier=max_demand_multiplier,
                        num_tasks_dependent=num_tasks_dependent,
                        full_flex_task_min=full_flex_task_min,
                        full_flex_task_max=full_flex_task_max,
                        semi_flex_task_min=semi_flex_task_min,
                        semi_flex_task_max=semi_flex_task_max,
                        fixed_task_min=fixed_task_min,
                        fixed_task_max=fixed_task_max,
                        inconvenience_cost_weight=inconvenience_cost_weight,
                        max_care_factor=max_care_factor,
                        household_id=household_id)
        household_tracker.new(method=scheduling_method)
        household_tracker.update(num_record=0, method=scheduling_method,
                                 demands=household_demand_profile, penalty=0)

        if write_to_file_path is not None:
            self.write_to_file(tasks=tasks, write_to_file_path=write_to_file_path, household_id=household_id)
        # print(f"Household{household_id} is created.")
        return tasks, household_tracker


    def write_to_file(self, tasks, write_to_file_path, household_id=0):
        write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
            else write_to_file_path + "/"
        path = Path(write_to_file_path)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)
        with open(f"{write_to_file_path}household{household_id}.txt", "w") as f:
            f.write(dumps(tasks, indent=1))
        f.close()
        print(f"{write_to_file_path}household{household_id}.txt written.")


    def schedule_household(self, prices, scheduling_method, household, num_intervals=no_intervals,
                   model=None, solver=None, search=None):
        prices = self.__convert_price(num_intervals, prices)

        def preprocessing():
            max_duration = max(durations)
            # this big cost and big cost * number_tasks need to be
            # smaller than the largest number that the solver can handle
            big_value = max_demand * max_duration * prices[-2] + \
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

        # read tasks
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
        max_demand = household[h_demand_limit]
        inconvenience_cost_weight = household[h_incon_weight]

        # begin scheduling
        objective_values, big_value = preprocessing()
        if "minizinc" in scheduling_method:
            model = file_cp_pre if model is None else model
            solver = solver_name if solver is None else solver
            search = f"int_search(actual_starts, {variable_selection}, {value_choice}, complete)" \
                if search is None else search
            succ_delays = [x[0] for x in list(household[h_succ_delay].values())]
            actual_starts, time_scheduling \
                = self.__minizinc_model(model_file=model, solver=solver, search=search,
                                        objective_values=objective_values, powers=powers, max_demand=max_demand,
                                        durations=durations, earliest_starts=earliest_starts,
                                        preferred_starts=preferred_starts,
                                        latest_ends=latest_ends, successors=successors, precedents=precedents,
                                        no_precedents=no_precedents, succ_delays=succ_delays, care_factors=care_factors,
                                        prices=prices, inconvenience_cost_weight=inconvenience_cost_weight,
                                        num_intervals=num_intervals)
        else:
            actual_starts, time_scheduling \
                = self.__ogsa(objective_values=objective_values, big_value=big_value,
                              powers=powers, durations=durations, preferred_starts=preferred_starts,
                              latest_ends=latest_ends, max_demand=max_demand,
                              successors=successors, precedents=precedents, succ_delays=succ_delays,
                              randomness=False, num_intervals=num_intervals)


        household_demand_profile = [0] * num_intervals
        for p, ast, dur in zip(powers, actual_starts, durations):
            for t in range(ast, ast + dur):
                household_demand_profile[t % num_intervals] += p

        # return results
        weighted_penalty_household = sum([abs(pst - ast) * cf for pst, ast, cf
                                          in
                                          zip(preferred_starts, actual_starts, care_factors)]) * inconvenience_cost_weight

        return {h_key: key, k0_demand: household_demand_profile, k0_starts: actual_starts,
                k0_penalty: weighted_penalty_household, k0_time: time_scheduling}


    def finalise_household(self, probability_distribution,
                           household_tracker_data=None, scheduling_method=None, num_schedule=0):
        if scheduling_method is None:
            scheduling_method = self.scheduling_method
        if household_tracker_data is None:
            household_tracker_data = self.household_tracker.data

        chosen_iter = choice(len(probability_distribution), size=1, p=probability_distribution)[0]
        chosen_demand_profile = household_tracker_data[scheduling_method][k0_demand][chosen_iter].copy()
        chosen_penalty = household_tracker_data[scheduling_method][k0_penalty][chosen_iter]

        if household_tracker_data is None:
            self.household_final.new(method=scheduling_method)
            self.household_final.update(num_record=num_schedule, method=scheduling_method,
                                        demands=chosen_demand_profile, penalty=chosen_penalty)
        return chosen_demand_profile, chosen_penalty


    def __convert_price(self, num_intervals, prices):
        num_periods = len(prices)
        num_intervals_period = int(num_intervals / num_periods)
        if num_periods != num_intervals:
            prices = [p for p in prices for _ in range(num_intervals_period)]
        else:
            prices = [p for p in prices]

        return prices


    def __new_task(self, mode_value, list_of_devices_power, pst_probabilities, max_care_factor,
                   scheduling_window_width, ):
        # ---------------------------------------------------------------------- #
        # mode_value:
        #       a parameter for generation the duration using the Rayleigh distribution
        # list_of_device_power:
        #       the list of commonly used devices' power, or the list of devices' power rates
        # pst_probabilities:
        #       the probability distribution used for sampling the PST
        # max_care_factor:
        #       the maximum care factor, e.g. 1, or 10 or 100
        # scheduling_window_width:
        #       "full", can be rescheduled to any time intervals;
        #       "fixed", cannot be moved;
        # ---------------------------------------------------------------------- #

        num_intervals = self.num_intervals
        num_periods = self.num_periods
        num_intervals_periods = self.num_intervals_periods

        # task power
        power = r.choice(list_of_devices_power)
        power = int(power * 1000)

        # task duration
        duration = max(1, int(random.rayleigh(mode_value, 1)[0]))

        # task preferred start time
        preferred_start_time = max(int(np.random.choice(a=num_periods, size=1, p=pst_probabilities)[0]) *
                                   num_intervals_periods + r.randint(-num_intervals_periods + 1, num_intervals_periods),
                                   0)
        preferred_start_time = min(preferred_start_time, num_intervals - 1)

        # task earliest starting time and latest finish time
        if scheduling_window_width == "full":
            earliest_start_time = 0
            latest_finish_time = num_intervals - 1 + duration
        elif scheduling_window_width == "fixed":
            earliest_start_time = preferred_start_time
            latest_finish_time = preferred_start_time + duration - 1
        else:
            earliest_start_time = r.randint(0, preferred_start_time)
            latest_finish_time = r.randint(preferred_start_time  + duration - 1, num_intervals - 1 + duration)

        # task care factor
        care_factor = int(r.choice([i for i in range(1, max_care_factor + 1)]))

        return power, duration, preferred_start_time, earliest_start_time, latest_finish_time, care_factor


    def new_tasks(self, num_intervals, preferred_demand_profile, list_of_devices_power,
                  max_demand_multiplier=maxium_demand_multiplier,
                  num_tasks_dependent=no_tasks_dependent,
                  full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
                  semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
                  fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
                  inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
                  household_id=0):
        # ---------------------------------------------------------------------- #
        # preferred_demand_profile:
        #       the demand profile for computing the probability distribution for sampling the preferred start times
        # list_of_devices_power:
        #       the power of the tasks
        # max_demand_multiplier:
        # num_tasks_dependent:
        # full_flex_task_min and full_flex_task_max:
        #       the minimum and the maximum number of fully flexible tasks
        # semi_flex_task_min and semi_flex_task_max:
        #       the minimum and the maximum number of semi-flexible tasks
        # fixed_task_min and care_f_weight:
        # inconvenience_cost_weight and max_care_factor:
        # num_intervals, num_periods and num_intervals_periods:
        # write_to_file:
        #       whether to write the created household data to a file
        # ---------------------------------------------------------------------- #

        pst_probabilities = [int(p) for p in preferred_demand_profile]
        sum_pst_probabilities = sum(pst_probabilities)
        pst_probabilities = [p / sum_pst_probabilities for p in pst_probabilities]

        # I meant mean value is 40 minutes
        mean_value = 40.0 / (24.0 * 60.0 / num_intervals)
        mode_value = sqrt(2 / pi) * mean_value

        # task details
        preferred_starts = []
        earliest_starts = []
        latest_ends = []
        durations = []
        powers = []
        care_factors = []
        household_demand_profile = [0] * num_intervals

        # tasks in the household
        def get_new_tasks(num_tasks, scheduling_window_width):
            for counter_j in range(num_tasks):
                demand, duration, p_start, e_start, l_finish, care_f \
                    = self.__new_task(mode_value, list_of_devices_power,
                                      pst_probabilities, max_care_factor, scheduling_window_width)

                powers.append(demand)
                durations.append(duration)
                preferred_starts.append(p_start)
                earliest_starts.append(e_start)
                latest_ends.append(l_finish)
                care_factors.append(care_f)
                # add this task demand to the household demand
                for d in range(duration):
                    household_demand_profile[(p_start + d) % num_intervals] += demand

        fixed_task_max = max(fixed_task_max, fixed_task_min)
        semi_flex_task_max = max(semi_flex_task_max, semi_flex_task_min)
        full_flex_task_max = max(full_flex_task_max, full_flex_task_min)

        num_fixed_tasks = r.randint(fixed_task_min, fixed_task_max)
        num_semi_flex_tasks = r.randint(semi_flex_task_min, semi_flex_task_max)
        num_full_flex_tasks = r.randint(full_flex_task_min, full_flex_task_max)

        get_new_tasks(num_fixed_tasks, "fixed")
        get_new_tasks(num_semi_flex_tasks, "semi")
        get_new_tasks(num_full_flex_tasks, "full")

        # set the household demand limit
        maximum_demand = sum(powers) * max_demand_multiplier

        # precedence among tasks
        precedors = dict()
        no_precedences = 0
        succ_delays = dict()

        def retrieve_precedes(list0):
            list3 = []
            for l in list0:
                if l in precedors:
                    list2 = precedors[l]
                    retrieved_list = retrieve_precedes(list2)
                    list3.extend(retrieved_list)
                else:
                    list3.append(l)
            return list3

        def add_precedes(task, previous, delay):
            if task not in precedors:
                precedors[task] = [previous]
                succ_delays[task] = [delay]
            else:
                precedors[task].append(previous)
                succ_delays[task].append(delay)

        num_total_tasks = num_full_flex_tasks + num_semi_flex_tasks + num_fixed_tasks
        for t in range(num_total_tasks - num_tasks_dependent, num_total_tasks):
            if r.choice([True, False]):
                previous_tasks = list(range(t))
                r.shuffle(previous_tasks)
                for prev in previous_tasks:
                    if preferred_starts[prev] + durations[prev] - 1 < preferred_starts[t] \
                            and earliest_starts[prev] + durations[prev] < latest_ends[t] - durations[t] + 1:

                        if prev not in precedors:
                            # feasible delay
                            succeding_delay = num_intervals - 1
                            add_precedes(t, prev, succeding_delay)
                            no_precedences += 1
                            break
                        else:
                            # find all precedors of this previous task
                            precs_prev = retrieve_precedes([prev])
                            precs_prev.append(prev)

                            precs_prev_duration = sum([durations[x] for x in precs_prev])
                            latest_pstart = preferred_starts[precs_prev[0]]
                            latest_estart = earliest_starts[precs_prev[0]]

                            if latest_pstart + precs_prev_duration - 1 < preferred_starts[t] \
                                    and latest_estart + precs_prev_duration < latest_ends[t] - durations[t] + 1:
                                succeding_delay = num_intervals - 1
                                add_precedes(t, prev, succeding_delay)
                                no_precedences += 1
                                break

        tasks = dict()
        if household_id is not None:
            tasks[h_key] = household_id
        tasks[h_psts] = preferred_starts
        tasks[h_ests] = earliest_starts
        tasks[h_lfs] = latest_ends
        tasks[h_durs] = durations
        tasks[h_powers] = powers
        tasks[h_cfs] = care_factors
        tasks[h_max_cf] = max_care_factor
        tasks[h_no_precs] = no_precedences
        tasks[h_precs] = precedors
        tasks[h_succ_delay] = succ_delays
        tasks[h_demand_limit] = maximum_demand
        tasks[h_incon_weight] = inconvenience_cost_weight
        tasks[k0_demand] = household_demand_profile

        return tasks, household_demand_profile


    def __existing_household(self, household_file):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #
        with open(household_file, 'r') as f:
            household = load(f)
        f.close()

        return household


    def __minizinc_model(self, model_file, solver, search,
                         objective_values, powers, max_demand, durations,
                         earliest_starts, preferred_starts, latest_ends,
                         successors, precedents, no_precedents, succ_delays,
                         care_factors, prices, inconvenience_cost_weight, num_intervals=None):
        if num_intervals is None:
            num_intervals = self.num_intervals

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

        if "ini" in model_type.lower():
            ins["prices"] = prices
            ins["preferred_starts"] = [ps + 1 for ps in preferred_starts]
            ins["earliest_starts"] = [es + 1 for es in earliest_starts]
            ins["latest_ends"] = [le + 1 for le in latest_ends]
            ins["care_factors"] = [cf * inconvenience_cost_weight for cf in care_factors]
        else:
            ins["run_costs"] = objective_values

        # solve problem model
        # result = ins.solve(timeout=timedelta(seconds=10))
        result = ins.solve()

        # process problem solution
        # obj = result.objective
        solution = result.solution.actual_starts
        actual_starts = [int(a) - 1 for a in solution]
        # if "cp" in solver_type:
        #     actual_starts = [int(a) - 1 for a in solution]
        # else:  # "mip" in solver_type:
        #     actual_starts = [sum([i * int(v) for i, v in enumerate(row)]) for row in solution]
        time = result.statistics["time"].total_seconds()



        return actual_starts, time


    def __ogsa(self, objective_values, big_value, powers, durations, preferred_starts, latest_ends, max_demand,
               successors, precedents, succ_delays, randomness=True, num_intervals=None):
        start_time = timeit.default_timer()
        if num_intervals is None:
            num_intervals = self.num_intervals

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
                if task_costs[j] < big_value and earliest_suc_lstart_w_delay < j < earliest_suc_lstart - duration + 1 \
                        and latest_pre_finish < j < latest_pre_finish_w_delay:
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
