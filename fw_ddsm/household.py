import pickle
from numpy.random import choice
from json import dumps, load
from pathlib import Path

from numpy import genfromtxt
from fw_ddsm.scripts import household_generation
from fw_ddsm.scripts import household_scheduling
from fw_ddsm.tracker import *


class Household:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)
        self.scheduling_method = ""
        self.tasks = dict()
        self.household_id = 0
        self.household_tracker = Tracker()
        self.household_final = Tracker()

    def read_household(self, scheduling_method, read_from_folder="households", household_id=0):
        self.scheduling_method = scheduling_method

        if not read_from_folder.endswith("/"):
            read_from_folder += "/"
        with open(f"{read_from_folder}household{household_id}.txt", 'r') as f:
            household = load(f)
        f.close()
        self.tasks = household
        self.household_id = household_id

        self.household_tracker = Tracker()
        self.household_tracker.new(name=f"h{household_id}")
        self.household_tracker.update(num_record=0, starts=self.tasks[h_psts], demands=self.tasks[s_demand], penalty=0)
        self.household_final = Tracker()
        self.household_final.new(name=f"h{household_id}_final")

        print(f"0. Household{household[h_key]} is read.")
        return self.tasks, self.household_tracker

    def new(self, num_intervals, scheduling_method,
            preferred_demand_profile=None, list_of_devices_power=None,
            preferred_demand_profile_csv=None, list_of_devices_power_csv=None,
            max_demand_multiplier=maximum_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_folder=None, household_id=0):

        self.scheduling_method = scheduling_method
        self.household_id = 0

        if preferred_demand_profile is None and preferred_demand_profile_csv is None:
            print("Please provide a preferred demand profile or the csv. ")
        if list_of_devices_power is None and list_of_devices_power_csv is None:
            print("Please provide the power rates of the tasks. ")
        if preferred_demand_profile_csv is not None:
            preferred_demand_profile = genfromtxt(preferred_demand_profile_csv, delimiter=',', dtype="float")
        if list_of_devices_power_csv is not None:
            list_of_devices_power = genfromtxt(list_of_devices_power_csv, delimiter=',', dtype="float")

        tasks, household_demand_profile, household_preferred_starts \
            = household_generation.new_household(num_intervals=num_intervals,
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

        if write_to_folder is not None:
            self.save_to_file(tasks=tasks, folder=write_to_folder, household_id=household_id)
        self.tasks = tasks.copy()

        self.household_tracker = Tracker()
        self.household_tracker.new(name=f"h{household_id}")
        self.household_tracker.update(num_record=0, starts=household_preferred_starts,
                                      demands=household_demand_profile, penalty=0)
        self.household_final = Tracker()
        self.household_final.new(name=f"h{household_id}_final")

        # print(f"Household{household_id} is created.")
        return self.tasks, self.household_tracker

    def save_to_file(self, tasks, folder, household_id=0):
        if not folder.endswith("/"):
            folder += "/"
        file_name = f"h{household_id}.txt"

        path = Path(folder)
        if not Path(folder).exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)
        with open(f"{folder}{file_name}", "w") as f:
            f.write(dumps(tasks, indent=1))
        f.close()

        print(f"0. {folder}{file_name} written.")

    def schedule(self, num_iteration, prices, model=None, solver=None, search=None, timeout=time_out):
        result = self.schedule_household(prices=prices,
                                         scheduling_method=self.scheduling_method,
                                         household=self.tasks,
                                         model=model, solver=solver, search=search, timeout=timeout)
        household_demand_profile = result[s_demand]
        weighted_penalty_household = result[s_penalty]
        household_start_times = result[s_starts]
        self.household_tracker.update(num_record=num_iteration,
                                      starts=household_start_times,
                                      demands=household_demand_profile,
                                      penalty=weighted_penalty_household)

        return household_demand_profile, weighted_penalty_household, household_start_times

    def schedule_household(self, prices, scheduling_method, household, num_intervals=no_intervals,
                           model=None, solver=None, search=None, timeout=time_out, print_done=False):

        prices = self.__convert_price(num_intervals, prices)

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
        objective_values, big_value \
            = household_scheduling.preprocessing(powers=powers, durations=durations, max_demand=max_demand,
                                                 prices=prices,
                                                 preferred_starts=preferred_starts,
                                                 earliest_starts=earliest_starts,
                                                 latest_ends=latest_ends,
                                                 care_factors=care_factors,
                                                 inconvenience_cost_weight=inconvenience_cost_weight,
                                                 max_care_factor=max_care_factor, num_intervals=no_intervals)
        if "minizinc" in scheduling_method:
            model = file_cp_pre if model is None else model
            solver = solver_name if solver is None else solver
            search = f"int_search(actual_starts, {variable_selection}, {value_choice}, complete)" \
                if search is None else search
            succ_delays = [x[0] for x in list(household[h_succ_delay].values())]
            actual_starts, time_scheduling \
                = household_scheduling.minizinc_model(model_file=model, solver=solver, search=search,
                                                      objective_values=objective_values, powers=powers,
                                                      max_demand=max_demand,
                                                      durations=durations, earliest_starts=earliest_starts,
                                                      preferred_starts=preferred_starts,
                                                      latest_ends=latest_ends, successors=successors,
                                                      precedents=precedents,
                                                      no_precedents=no_precedents, succ_delays=succ_delays,
                                                      care_factors=care_factors,
                                                      prices=prices,
                                                      inconvenience_cost_weight=inconvenience_cost_weight,
                                                      num_intervals=num_intervals, timeout=timeout)
        else:
            actual_starts, time_scheduling \
                = household_scheduling.ogsa(objective_values=objective_values, big_value=big_value,
                                            powers=powers, durations=durations, preferred_starts=preferred_starts,
                                            latest_ends=latest_ends, max_demand=max_demand,
                                            successors=successors, precedents=precedents, succ_delays=succ_delays,
                                            randomness=False, num_intervals=num_intervals)

        household_demand_profile = [0] * num_intervals
        for p, ast, dur in zip(powers, actual_starts, durations):
            for t in range(ast, ast + dur):
                household_demand_profile[t % num_intervals] += p

        weighted_penalty_household \
            = inconvenience_cost_weight * sum([abs(pst - ast) * cf
                                               for pst, ast, cf in zip(preferred_starts, actual_starts, care_factors)])

        if print_done:
            print(f"Household {key}, {actual_starts}. ")
        return {h_key: key, s_demand: household_demand_profile, s_starts: actual_starts,
                s_penalty: weighted_penalty_household, t_time: time_scheduling}

    def finalise_household(self, probability_distribution,
                           household_tracker_data=None, num_schedule=0):

        if household_tracker_data is None:
            household_tracker_data = self.household_tracker.data

        chosen_iter = choice(len(probability_distribution), size=1, p=probability_distribution)[0]
        chosen_demand_profile = household_tracker_data[s_demand][chosen_iter].copy()
        chosen_penalty = household_tracker_data[s_penalty][chosen_iter]
        chosen_start_times = household_tracker_data[s_starts][chosen_iter].copy()

        if household_tracker_data is None:
            self.household_final.update(num_record=num_schedule, starts=chosen_start_times,
                                        demands=chosen_demand_profile, penalty=chosen_penalty)
        return chosen_demand_profile, chosen_penalty, chosen_start_times

    def __convert_price(self, num_intervals, prices):
        num_periods = len(prices)
        num_intervals_period = int(num_intervals / num_periods)
        if num_periods != num_intervals:
            prices = [p for p in prices for _ in range(num_intervals_period)]
        else:
            prices = [p for p in prices]

        return prices
