import random as r
from numpy import sqrt, pi, random
from fw_ddsm.parameter import *


def new_task(
        mode_value, list_of_devices_power, pst_probabilities, max_care_factor, scheduling_window_width,
        num_intervals=no_intervals, num_periods=no_periods, num_intervals_periods=no_intervals_periods
):
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

    # task power
    power = r.choice(list_of_devices_power)
    power = int(power * 1000)

    # task duration
    duration = max(1, int(random.rayleigh(mode_value, 1)[0]))

    # task preferred start time
    preferred_start_time = max(int(random.choice(a=num_periods, size=1, p=pst_probabilities)[0]) *
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
        latest_finish_time = r.randint(preferred_start_time + duration - 1, num_intervals - 1 + duration)

    # task care factor
    care_factor = int(r.choice([i for i in range(1, max_care_factor + 1)]))

    return power, duration, preferred_start_time, earliest_start_time, latest_finish_time, care_factor


def new_dependent_tasks(num_intervals, num_tasks_dependent, num_total_tasks,
                        preferred_starts, durations, earliest_starts, latest_ends, ensure_dependent=False):
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

    for t in range(num_total_tasks - num_tasks_dependent, num_total_tasks):
        if r.choice([True, ensure_dependent]):
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

    return no_precedences, precedors, succ_delays


def new_household(
        preferred_demand_profile, list_of_devices_power,
        num_intervals=no_intervals, num_periods=no_periods, num_intervals_periods=no_intervals_periods,
        max_demand_multiplier=maximum_demand_multiplier,
        num_tasks_dependent=no_tasks_dependent, ensure_dependent=False,
        full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
        semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
        fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
        inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
        household_id=0
):
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
                = new_task(mode_value, list_of_devices_power, pst_probabilities,
                           max_care_factor, scheduling_window_width,
                           num_intervals=num_intervals, num_periods=num_periods,
                           num_intervals_periods=num_intervals_periods)

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
    num_total_tasks = num_full_flex_tasks + num_semi_flex_tasks + num_fixed_tasks
    no_precedences, precedors, succ_delays \
        = new_dependent_tasks(num_intervals, num_tasks_dependent, num_total_tasks,
                              preferred_starts, durations, earliest_starts, latest_ends, ensure_dependent)

    household = dict()
    if household_id is not None:
        household[h_key] = household_id
    household[h_psts] = preferred_starts
    household[h_ests] = earliest_starts
    household[h_lfs] = latest_ends
    household[h_durs] = durations
    household[h_powers] = powers
    household[h_cfs] = care_factors
    household[h_max_cf] = max_care_factor
    household[h_no_precs] = no_precedences
    household[h_precs] = precedors
    household[h_succ_delay] = succ_delays
    household[h_demand_limit] = maximum_demand
    household[h_incon_weight] = inconvenience_cost_weight
    household[s_demand] = household_demand_profile

    return household, household_demand_profile, preferred_starts
