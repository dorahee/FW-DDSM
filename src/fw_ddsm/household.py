from numpy.random import choice
from json import dumps, load
from pathlib import Path

from numpy import genfromtxt
from src.fw_ddsm.functions import household_scheduling, household_generation
from src.fw_ddsm.tracker import *
from src.fw_ddsm.common import entity


class Household(entity.Entity):

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        super().__init__(num_intervals, num_periods)
        self.tasks_scheduling_method = ""
        self.household_id = 0

    def read_household(self, tasks_scheduling_method, read_from_folder="households", household_id=0):

        # record the scheduling method
        self.tasks_scheduling_method = tasks_scheduling_method

        # read household task details and battery details

        with open(f"{read_from_folder}h{household_id}.txt", 'r') as f:
            household_details = load(f)
        f.close()
        self.data_dict = household_details
        self.household_id = household_id

        # create a household tracker for results at each iteration
        self.tracker = Tracker()
        self.tracker.new(name=f"h{household_id}")
        self.tracker.update(num_record=0, tasks_starts=self.data_dict[h_psts],
                            demands=self.data_dict[s_demand], penalty=0,
                            battery_profile=self.data_dict[b_profile])

        # create a tracker for the final schedules
        self.final = Tracker()
        self.final.new(name=f"h{household_id}_final")

        # write a message when done
        print(f"0. Household{household_details[h_key]} is read.")
        return self.data_dict, self.tracker

    def new(self, num_intervals, tasks_scheduling_method,
            preferred_demand_profile=None, list_of_devices_power=None,
            file_preferred_demand_profile=None, file_list_of_devices_power=None,
            max_demand_multiplier=maximum_demand_multiplier,
            num_tasks_dependent=no_dependent_tasks,
            full_flex_task_min=min_full_flex_tasks, full_flex_task_max=0,
            semi_flex_task_min=min_semi_flex_tasks, semi_flex_task_max=0,
            fixed_task_min=min_fixed_tasks, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_folder=None, household_id=0,
            capacity_max=battery_capacity_max, capacity_min=battery_capacity_min,
            power=battery_power, efficiency=battery_efficiency):

        self.household_id = 0

        # read data for generating new household details
        if preferred_demand_profile is None and file_preferred_demand_profile is None:
            print("Please provide a preferred demand profile or the csv. ")
        if list_of_devices_power is None and file_list_of_devices_power is None:
            print("Please provide the power rates of the tasks. ")
        if file_preferred_demand_profile is not None:
            preferred_demand_profile = genfromtxt(file_preferred_demand_profile, delimiter=',', dtype="float")
        if file_list_of_devices_power is not None:
            list_of_devices_power = genfromtxt(file_list_of_devices_power, delimiter=',', dtype="float")

        # generate details of a new household
        household_details \
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
                                                 household_id=household_id,
                                                 capacity_max=capacity_max,
                                                 capacity_min=capacity_min,
                                                 power=power,
                                                 efficiency=efficiency)

        # write the new household details to a file if needed
        if write_to_folder is not None:
            self.save_to_file(tasks=household_details, folder=write_to_folder, household_id=household_id)
        self.data_dict = household_details.copy()

        # create a new tracker for the results at each iteration and save the initial details
        self.tracker = Tracker()
        self.tracker.new(name=f"h{household_id}")
        self.tracker.update(num_record=0, tasks_starts=household_details[h_psts],
                            demands=household_details[s_demand], penalty=0,
                            battery_profile=household_details[b_profile])

        # create a tracker for the final schedules
        self.final = Tracker()
        self.final.new(name=f"h{household_id}_final")

        # print(f"Household{household_id} is created.")
        return self.data_dict, self.tracker

    # def save_to_file(self, tasks, folder, household_id=0):
    #     if not folder.endswith("/"):
    #         folder += "/"
    #     file_name = f"h{household_id}.txt"
    #
    #     path = Path(folder)
    #     if not Path(folder).exists():
    #         path.mkdir(mode=0o777, parents=True, exist_ok=False)
    #     with open(f"{folder}{file_name}", "w") as f:
    #         f.write(dumps(tasks, indent=1))
    #     f.close()
    #
    #     print(f"0. {folder}{file_name} written.")

    def schedule(self, num_iteration, prices,
                 num_intervals=None,
                 household_details=None,
                 tasks_scheduling_method=None,
                 tasks_model=None, tasks_solver=None, tasks_search=None,
                 use_battery=False, battery_model=None, battery_solver=None,
                 timeout=time_out,
                 fully_charge_time=fully_charge_hour,
                 update_tracker=True,
                 print_upon_completion=False):

        if tasks_scheduling_method is None:
            tasks_scheduling_method = self.tasks_scheduling_method
        if household_details is None:
            household_details = self.data_dict
        if num_intervals is None:
            num_intervals = no_intervals
        if use_battery:
            battery_model = file_mip_battery if battery_model is None else battery_model
            battery_solver = "mip" if battery_solver is None else battery_solver

        # read household ID
        key = household_details[h_key]

        # schedule tasks
        tasks_result = self.schedule_tasks(prices=prices,
                                           method=tasks_scheduling_method,
                                           household=household_details,
                                           num_intervals=num_intervals,
                                           model=tasks_model,
                                           solver=tasks_solver,
                                           search=tasks_search,
                                           timeout=timeout,
                                           print_upon_completion=print_upon_completion)
        tasks_demand_profile = tasks_result[s_demand]  # check whether it is demand or consumption
        tasks_weighted_penalty = tasks_result[s_penalty]
        tasks_start_times = tasks_result[s_starts]
        tasks_time = tasks_result[t_time]
        tasks_preferred_times = tasks_result[h_psts]

        # schedule a battery if needed
        if use_battery:
            # print("Household", key)
            battery_result = self.schedule_battery(household=household_details,
                                                   existing_demands=tasks_demand_profile,
                                                   prices=prices,
                                                   model=battery_model,
                                                   solver=battery_solver,
                                                   num_intervals=num_intervals,
                                                   fully_charge_time=fully_charge_time,
                                                   print_upon_completion=print_upon_completion)
            battery_profile = battery_result[b_profile]
            household_demand_profile = [x + y for x, y in zip(tasks_demand_profile, battery_profile)]
            battery_time = battery_result[t_time]
        else:
            household_demand_profile = tasks_demand_profile[:]
            battery_profile = [0] * num_intervals
            battery_time = 0

        time_total = tasks_time + battery_time

        # update household tracker
        if update_tracker:
            self.tracker.update(num_record=num_iteration,
                                tasks_starts=tasks_start_times,
                                demands=household_demand_profile,
                                penalty=tasks_weighted_penalty,
                                battery_profile=battery_profile)

        return {h_key: key,
                s_demand: household_demand_profile,
                s_penalty: tasks_weighted_penalty,
                s_starts: tasks_start_times,
                h_psts: tasks_preferred_times,
                b_profile: battery_profile,
                t_time: time_total}

    def schedule_tasks(self, prices, method, household, num_intervals=no_intervals,
                       model=None, solver=None, search=None, timeout=time_out, print_upon_completion=False):

        prices = self.convert_price(num_intervals, prices)

        # read details of tasks
        key = household[h_key]
        powers = household[h_powers]
        durations = household[h_durs]
        earliest_starts = household[h_ests]
        latest_ends = household[h_lfs]
        preferred_starts = household[h_psts]
        care_factors = household[h_cfs]
        max_care_factor = household[h_max_cf]
        precedents = [x[0] for x in list(household[h_precs].values())]
        successors = [int(suc) for suc in list(household[h_precs].keys())]
        succ_delays = household[h_succ_delay]
        no_precedents = household[h_no_precs]
        max_demand = household[h_demand_limit]
        inconvenience_cost_weight = household[h_incon_weight]
        par_cost_weight = household[p_par_weight]

        # preprocess data
        objective_values, big_value \
            = household_scheduling.tasks_preprocessing(powers=powers, durations=durations, max_demand=max_demand,
                                                       prices=prices,
                                                       preferred_starts=preferred_starts,
                                                       earliest_starts=earliest_starts,
                                                       latest_ends=latest_ends,
                                                       care_factors=care_factors,
                                                       inconvenience_cost_weight=inconvenience_cost_weight,
                                                       max_care_factor=max_care_factor, num_intervals=no_intervals)

        # choose a method to schedule: optimal or heuristic
        if "minizinc" in method:
            model = file_cp_pre if model is None else model
            solver = tasks_solver_name if solver is None else solver
            search = f"int_search(actual_starts, {variable_selection}, {value_choice}, complete)" \
                if search is None else search
            succ_delays = [x[0] for x in list(household[h_succ_delay].values())]
            actual_starts, time_scheduling_tasks \
                = household_scheduling.tasks_minizinc(model_file=model, solver=solver,
                                                      search=search,
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
            succ_delays = {int(k): v for k, v in succ_delays.items()}
            actual_starts, time_scheduling_tasks \
                = household_scheduling.tasks_ogsa(objective_values=objective_values, big_value=big_value,
                                                  powers=powers, durations=durations, preferred_starts=preferred_starts,
                                                  latest_ends=latest_ends, max_demand=max_demand,
                                                  successors=successors, precedents=precedents, succ_delays=succ_delays,
                                                  randomness=False, num_intervals=num_intervals)

        # process the results: generate the household demand profile given the task start times
        household_demand_profile = [0] * num_intervals
        for p, ast, dur in zip(powers, actual_starts, durations):
            for t in range(ast, ast + dur):
                household_demand_profile[t % num_intervals] += p

        # calculate the inconvenience cost for this household
        weighted_penalty_household \
            = inconvenience_cost_weight * sum([abs(pst - ast) * cf
                                               for pst, ast, cf in zip(preferred_starts, actual_starts, care_factors)])

        # print a message when done if needed
        if print_upon_completion:
            print(f"Household {key}, {actual_starts}. ")

        # return the key information
        return {h_key: key, s_demand: household_demand_profile,
                h_psts: preferred_starts, s_starts: actual_starts,
                s_penalty: weighted_penalty_household, t_time: time_scheduling_tasks}

    def schedule_battery(self, household, existing_demands, prices,
                         model=None, solver=None,
                         num_intervals=no_intervals, fully_charge_time=fully_charge_hour,
                         print_upon_completion=False):

        model = file_mip_battery if model is None else model
        solver = "mip" if solver is None else solver

        # read household ID
        key = household[h_key]

        # read details of the battery
        capacity_max = household[b_cap_max]
        capacity_min = household[b_cap_min]
        power_max = household[b_power]
        efficiency = household[b_eff]
        par_cost_weight = household[p_par_weight]

        # schedule the battery
        battery_profile, time_scheduling_battery \
            = household_scheduling.battery_mip(model_file=model, solver=solver, existing_demands=existing_demands,
                                               capacity_max=capacity_max, capacity_min=capacity_min,
                                               power_max=power_max, efficiency=efficiency,
                                               prices=prices, par_cost_weight=par_cost_weight,
                                               fully_charge_time=fully_charge_time,
                                               num_intervals=num_intervals, timeout=time_out)

        if print_upon_completion:
            print(f"Household {key}, {battery_profile}. ")

        # return key information
        return {h_key: key, b_profile: battery_profile, t_time: time_scheduling_battery}

    def finalise_household(self, probability_distribution,
                           household_tracker_data=None, num_schedule=0):

        if household_tracker_data is None:
            household_tracker_data = self.tracker.data

        chosen_iter = choice(len(probability_distribution), size=1, p=probability_distribution)[0]
        chosen_demand_profile = household_tracker_data[s_demand][chosen_iter].copy()
        chosen_penalty = household_tracker_data[s_penalty][chosen_iter]
        chosen_start_times = household_tracker_data[s_starts][chosen_iter].copy()
        if 1 in household_tracker_data[b_profile]:
            chosen_battery_profile = household_tracker_data[b_profile][chosen_iter].copy()
        else:
            chosen_battery_profile = [0] * len(chosen_demand_profile)

        if household_tracker_data is None:
            self.final.update(num_record=num_schedule, tasks_starts=chosen_start_times,
                              demands=chosen_demand_profile, penalty=chosen_penalty,
                              battery_profile=chosen_battery_profile)
        return chosen_demand_profile, chosen_penalty, chosen_start_times, chosen_battery_profile

