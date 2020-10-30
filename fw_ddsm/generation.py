import random as r
import numpy as np
from numpy import sqrt, pi, random
from pandas import read_csv
from json import dumps, loads, load
from pathlib import Path
from numpy import genfromtxt
from more_itertools import grouper
import pickle
from fw_ddsm.parameter import *
from fw_ddsm.cfunctions import average


def new_pricing_table(normalised_pricing_table_csv, demand_level_scalar, num_periods=48):

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


def new_task(mode_value, list_of_devices_power,
             pst_probabilities, max_care_factor, scheduling_window_width,
             num_intervals=no_intervals, num_periods=no_periods, num_intervals_periods=no_intervals_periods):

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


def new_household(preferred_demand_profile, list_of_devices_power,
                  max_demand_multiplier=maxium_demand_multiplier, num_tasks_dependent=no_tasks_dependent,
                  full_flex_task_min=no_tasks_min, full_flex_task_max=no_tasks_max,
                  semi_flex_task_min=0, semi_flex_task_max=0,
                  fixed_task_min=0, fixed_task_max=0,
                  inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
                  num_intervals=no_intervals, num_periods=no_periods, num_intervals_periods=no_intervals_periods,
                  write_to_file_path=None, id=0):

    # preferred_demand_profile:
    #       the demand profile used for computing the probability distribution for sampling the preferred start times
    # list_of_devices_power:
    #       the power of the tasks
    # max_demand_multiplier:
    # num_tasks_dependent:
    # full_flex_task_min and full_flex_task_max:
    # semi_flex_task_min and semi_flex_task_max:
    # fixed_task_min and care_f_weight:
    # inconvenience_cost_weight and max_care_factor:
    # num_intervals, num_periods and num_intervals_periods:
    # write_to_file: whether to write the created household data to a file

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
                = new_task(mode_value, list_of_devices_power,
                           pst_probabilities, max_care_factor, scheduling_window_width,
                           num_intervals, num_periods, num_intervals_periods)

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
    maximum_demand = max(powers) * max_demand_multiplier

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

    household = dict()
    household[h_psts] = preferred_starts
    household[h_ests] = earliest_starts
    household[h_lfs] = latest_ends
    household[h_durs] = durations
    household[h_powers] = powers
    household[h_cfs] = care_factors
    household[h_no_precs] = no_precedences
    household[h_precs] = precedors
    household[h_succ_delay] = succ_delays
    household[h_max_demand] = maximum_demand
    household[h_demand_profile] = household_demand_profile
    household[h_incon_weight] = inconvenience_cost_weight

    if write_to_file_path is not None:
        write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
            else write_to_file_path + "/"
        path = Path(write_to_file_path)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)
        with open(f"{write_to_file_path}household{id}.txt", "w") as f:
            f.write(dumps(household, indent=1))
        f.close()
        print(f"{write_to_file_path}household{id}.json written.")

    return household


def existing_household(household_json_file):
    with open(household_json_file, 'r') as f:
        household = load(f)
    f.close()

    return household


def new_community(num_households, algorithms_choices, file_probability, file_demand_list,
                  num_intervals=no_intervals, num_periods=no_periods, num_intervals_periods=no_intervals_periods,
                  write_to_file_path=None):

    preferred_demand_profile = genfromtxt(file_probability, delimiter=',', dtype="float")
    list_of_devices_power = genfromtxt(file_demand_list, delimiter=',', dtype="float")

    households = dict()
    community_demand_profile = [0] * num_intervals
    for h in range(num_households):
        household = new_household(preferred_demand_profile, list_of_devices_power,
                                  num_intervals=num_intervals, num_periods=num_periods,
                                  num_intervals_periods=num_intervals_periods,
                                  full_flex_task_min=3, semi_flex_task_min=3, fixed_task_min=5,
                                  num_tasks_dependent=3)
        household_profile = household[h_demand_profile]
        household["key"] = h
        household[k0_starts] = dict()
        household[k0_demand] = dict()
        household[k0_cost] = dict()
        household[k0_penalty] = dict()
        household[k0_obj] = dict()
        household[k0_final] = dict()

        for k in algorithms_choices.keys():
            household[k0_starts][k] = dict()
            household[k0_penalty][k] = dict()
            household[k0_final][k] = dict()
            household[k0_demand][k] = dict()

            household[k0_starts][k][0] = household[h_psts]
            household[k0_penalty][k][0] = 0
            household[k0_demand][k][0] = household_profile

        households[h] = household.copy()
        community_demand_profile = [x + y for x, y in zip(household_profile, community_demand_profile)]

    community_demand_profile2 = [sum(x) for x in grouper(num_intervals_periods, community_demand_profile)]
    max_demand = max(community_demand_profile2)
    total_demand = sum(community_demand_profile2)
    par = round(max_demand / average(community_demand_profile2), 2)

    community_tracks = dict()
    for k1, v1 in algorithms_choices.items():
        for v2 in v1.values():
            community_tracks[v2] = dict()
            community_tracks[v2][k0_demand] = dict()
            community_tracks[v2][k0_demand_max] = dict()
            community_tracks[v2][k0_demand_total] = dict()
            community_tracks[v2][k0_par] = dict()
            community_tracks[v2][k0_penalty] = dict()
            community_tracks[v2][k0_final] = dict()

            community_tracks[v2][k0_demand][0] = community_demand_profile2
            community_tracks[v2][k0_demand_max][0] = max_demand
            community_tracks[v2][k0_demand_total][0] = total_demand
            community_tracks[v2][k0_par][0] = par
            community_tracks[v2][k0_penalty][0] = 0

    # write household data and area data into files
    if write_to_file_path is not None:
        write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
            else write_to_file_path + "/"
        path = Path(write_to_file_path)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        with open(f"{write_to_file_path}households.pkl", 'wb+') as f:
            pickle.dump(households, f, pickle.HIGHEST_PROTOCOL)
        f.close()

        with open(f"{write_to_file_path}community_track.pkl", 'wb+') as f:
            pickle.dump(community_tracks, f, pickle.HIGHEST_PROTOCOL)
        f.close()

    return households, community_tracks


def existing_community(file_path, inconvenience_cost_weight=None):
    file_path = file_path if file_path.endswith("/") else file_path + "/"

    with open(file_path + "households" + '.pkl', 'rb') as f:
        households = pickle.load(f)
    f.close()

    if inconvenience_cost_weight is not None:
        for household in households.values():
            household["care_factor_weight"] = inconvenience_cost_weight

    with open(file_path + "community_track" + '.pkl', 'rb') as f:
        community_tracks = pickle.load(f)
    f.close()

    return households, community_tracks