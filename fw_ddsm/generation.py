from numpy import genfromtxt
import random as r
import numpy as np
from numpy import sqrt, pi, random
from fw_ddsm.parameter import *
from pandas import read_csv


def new_pricing_table(num_periods, normalised_pricing_table_csv, demand_level_scalar):

    # normalised_pricing_table_csv:
    #       the path of the CSV file of the normalised pricing table
    # demand_level_scalar:
    #       the scalar for rescaling the normalised demand levels

    csv_table = read_csv(normalised_pricing_table_csv, header=None)
    num_levels = len(csv_table.index)
    csv_table.loc[num_levels + 1] = [csv_table[0].values[-1] * 10] + [demand_level_scalar for _ in range(num_periods)]

    zero_digit = 2
    pricing_table = dict()
    pricing_table[k0_price_levels] = list(csv_table[0].values)
    pricing_table[k0_demand_table] = dict()
    pricing_table[k0_demand_table] = \
        {period:
            {level:
                round(csv_table[period + 1].values[level] * demand_level_scalar, -zero_digit)
             for level in range(len(csv_table[period + 1]))}
         for period in range(num_periods)}

    return pricing_table


def new_task(num_intervals, num_periods, num_intervals_periods, mode_value, list_of_devices_power,
             pst_probabilities, max_care_factor, scheduling_window_width):

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

    # task power
    power = r.choice(list_of_devices_power)
    power = int(power * 1000)

    # task duration
    duration = max(1, int(random.rayleigh(mode_value, 1)[0]))

    # task preferred start time
    preferred_start_time = max(int(np.random.choice(a=num_periods, size=1, p=pst_probabilities)[0]) *
                               num_intervals_periods + r.randint(-num_intervals_periods + 1, num_intervals_periods), 0)
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
        latest_finish_time = num_intervals - 1 + duration

    # task care factor
    care_factor = int(r.choice([i for i in range(1, max_care_factor + 1)]))

    return power, duration, preferred_start_time, earliest_start_time, latest_finish_time, care_factor


def new_household(num_intervals, num_periods, num_intervals_periods, num_tasks_min, num_tasks_max, num_tasks_dependent,
                  pst_probabilities, max_demand_mul, max_care_factor, devices_power_file, inconvenience_cost_weight):
    pst_probabilities_short = [int(p) for p in pst_probabilities[0]]
    sum_t = sum(pst_probabilities_short)
    pst_probabilities_short = [p / sum_t for p in pst_probabilities_short]

    list_of_devices_power = genfromtxt(devices_power_file, delimiter=',', dtype="float")

    # I meant mean value is 40 minutes
    mean_value = 40.0 / (24.0 * 60.0 / num_intervals)
    mode_value = sqrt(2 / pi) * mean_value

    # task details
    preferred_starts = []
    earliest_starts = []
    latest_ends = []
    durations = []
    demands = []
    care_factors = []
    aggregated_loads = [0] * num_intervals

    # tasks in the household
    num_tasks = r.randint(num_tasks_min, num_tasks_max)
    for counter_j in range(num_tasks):
        demand, duration, p_start, e_start, l_finish, care_f \
            = new_task(num_intervals, num_periods, num_intervals_periods, mode_value, list_of_devices_power,
                       pst_probabilities_short, max_care_factor, "full")
        demands.append(demand)
        durations.append(duration)
        preferred_starts.append(p_start)
        earliest_starts.append(e_start)
        latest_ends.append(l_finish)
        care_factors.append(care_f)
        # add this task demand to the household demand
        for d in range(duration):
            aggregated_loads[(p_start + d) % num_intervals] += demand
    # set the household demand limit
    maximum_demand = max(demands) * max_demand_mul

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

    for t in range(num_tasks - num_tasks_dependent, num_tasks):
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

    household = dict()
    household[h_psts] = preferred_starts
    household[h_ests] = earliest_starts
    household[h_lfs] = latest_ends
    household[h_durs] = durations
    household[h_powers] = demands
    household[h_cfs] = care_factors
    household[h_no_precs] = no_precedences
    household[h_precs] = precedors
    household[h_succ_delay] = succ_delays
    household[h_max_demand] = maximum_demand
    household[h_demand_profile] = aggregated_loads
    household[h_incon_weight] = inconvenience_cost_weight

    # todo - write a test script

    return household


