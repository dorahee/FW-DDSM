from multiprocessing import Pool, cpu_count
import concurrent.futures
import pickle
from time import time
from fw_ddsm.household import *
from fw_ddsm.tracker import *
from fw_ddsm.scripts import household_generation, household_scheduling


class Community:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)
        self.num_households = 0
        self.households = dict()
        self.tracker = Tracker()
        self.final = Tracker()
        self.scheduling_method = ""
        self.preferred_demand_profile = []

    def read(self, scheduling_method, read_from_folder="data/",
             inconvenience_cost_weight=None, num_dependent_tasks=None, ensure_dependent=False,
             date_time=None):
        if not read_from_folder.endswith("/"):
            read_from_folder += "/"

        self.households = dict()
        self.households, self.preferred_demand_profile \
            = self.__existing_households(file_path=read_from_folder, date_time=date_time,
                                         inconvenience_cost_weight=inconvenience_cost_weight,
                                         num_dependent_tasks=num_dependent_tasks, ensure_dependent=ensure_dependent)
        if s_demand in self.households:
            self.num_households = len(self.households) - 1
        self.new_community_tracker(scheduling_method=scheduling_method)
        print("0. The community is read. ")

        return self.preferred_demand_profile

    def new(self, file_preferred_demand_profile, file_demand_list, scheduling_method,
            num_intervals=no_intervals, num_households=no_households,
            max_demand_multiplier=maximum_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent, ensure_dependent=False,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_file_path=None, date_time=None):

        self.scheduling_method = scheduling_method
        self.num_intervals = num_intervals
        self.num_households = num_households

        households = dict()
        aggregate_demand_profile = [0] * num_intervals
        preferred_demand_profile = genfromtxt(file_preferred_demand_profile, delimiter=',', dtype="float")
        list_of_devices_power = genfromtxt(file_demand_list, delimiter=',', dtype="float")
        for h in range(num_households):
            household, household_demand_profile, preferred_starts \
                = household_generation.new_household(num_intervals=num_intervals,
                                                     preferred_demand_profile=preferred_demand_profile,
                                                     list_of_devices_power=list_of_devices_power,
                                                     max_demand_multiplier=max_demand_multiplier,
                                                     num_tasks_dependent=num_tasks_dependent,
                                                     ensure_dependent=ensure_dependent,
                                                     full_flex_task_min=full_flex_task_min,
                                                     full_flex_task_max=full_flex_task_max,
                                                     semi_flex_task_min=semi_flex_task_min,
                                                     semi_flex_task_max=semi_flex_task_max,
                                                     fixed_task_min=fixed_task_min,
                                                     fixed_task_max=fixed_task_max,
                                                     inconvenience_cost_weight=inconvenience_cost_weight,
                                                     max_care_factor=max_care_factor,
                                                     household_id=h)
            aggregate_demand_profile = [x + y for x, y in zip(household_demand_profile, aggregate_demand_profile)]

            households[h] = household.copy()
            households[h][k_tracker] = Tracker()
            households[h][k_tracker].new()
            households[h][k_tracker].update(num_record=0, starts=preferred_starts,
                                            demands=household_demand_profile, penalty=0)

        self.households = households
        self.preferred_demand_profile = aggregate_demand_profile
        self.new_community_tracker(scheduling_method=scheduling_method)

        if write_to_file_path is not None:
            self.write_to_file(write_to_file_path, date_time=date_time)
        else:
            self.write_to_file("data/")
        print("0. The community is created. ")
        return aggregate_demand_profile

    def new_community_tracker(self, scheduling_method):
        self.tracker = Tracker()
        self.tracker.new(name=f"{scheduling_method}_community")
        self.tracker.update(num_record=0, penalty=0, run_time=0)
        self.final = Tracker()
        self.final.new(name=f"{scheduling_method}_community_final")
        self.tracker.update(num_record=0, penalty=0, run_time=0)

    def write_to_file(self, folder, date_time=None):

        if not folder.endswith("/"):
            folder += "/"
        folder += "data/"
        path = Path(folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        self.households[s_demand] = self.preferred_demand_profile
        if date_time is None:
            file_name = f"{folder}{file_community_pkl}"
        else:
            file_name = f"{folder}{date_time}_{file_community_pkl}"
        with open(file_name, 'wb+') as f:
            pickle.dump(self.households, f, pickle.HIGHEST_PROTOCOL)
        del self.households[s_demand]
        f.close()

    def schedule(self, num_iteration, prices, scheduling_method, model=None, solver=None, search=None, households=None,
                 num_cpus=None, timeout=time_out, print_done=False):

        prices = self.__convert_price(prices)

        if households is None:
            households = self.households
        results = self.__schedule_multiple_processing(households=households, prices=prices,
                                                      scheduling_method=scheduling_method,
                                                      model=model, solver=solver, search=search,
                                                      num_cpus=num_cpus, timeout=timeout, print_done=print_done)

        aggregate_demand_profile, weighted_total_inconvenience, time_scheduling_iteration \
            = self.__retrieve_scheduling_results(results=results, num_iteration=num_iteration)

        self.tracker.update(num_record=num_iteration, penalty=weighted_total_inconvenience,
                            run_time=time_scheduling_iteration)

        return aggregate_demand_profile, weighted_total_inconvenience, time_scheduling_iteration

    def finalise_schedule(self, scheduling_method, start_probability_distribution, num_sample=0):
        final_aggregate_demand_profile = [0] * self.num_intervals
        final_total_inconvenience = 0
        total_demand = 0
        for household in self.households.values():
            chosen_demand_profile, chosen_penalty, chosen_start_times \
                = Household.finalise_household(self=Household(), household_tracker_data=household[k_tracker].data,
                                               probability_distribution=start_probability_distribution)
            final_aggregate_demand_profile \
                = [x + y for x, y in zip(chosen_demand_profile, final_aggregate_demand_profile)]
            final_total_inconvenience += chosen_penalty
            total_demand += sum(chosen_demand_profile)

            household_id = household[h_key]
            if k_tracker_final not in self.households[household_id]:
                self.households[household_id][k_tracker_final] = Tracker()
                self.households[household_id][k_tracker_final].new()
            self.households[household_id][k_tracker_final].update(num_record=num_sample, starts=chosen_start_times,
                                                                  demands=chosen_demand_profile, penalty=chosen_penalty)

        self.final.update(num_record=num_sample, demands=final_aggregate_demand_profile,
                          penalty=final_total_inconvenience)

        return final_aggregate_demand_profile, final_total_inconvenience

    def __convert_price(self, prices):
        num_periods = len(prices)
        num_intervals_period = int(self.num_intervals / num_periods)
        if num_periods != self.num_intervals:
            prices = [p for p in prices for _ in range(num_intervals_period)]
        else:
            prices = [p for p in prices]

        return prices

    def __existing_households(self, file_path, date_time=None, inconvenience_cost_weight=None,
                              num_dependent_tasks=None, ensure_dependent=False):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #
        if date_time is None:
            file_name = f"{file_path}{file_community_pkl}"
        else:
            file_name = f"{file_path}data/{date_time}_{file_community_pkl}"
        with open(file_name, 'rb') as f:
            households = pickle.load(f)
        f.close()
        preferred_demand_profile = households.pop(s_demand)

        for key, household in households.items():
            household_tracker = Tracker()
            household_tracker.new()
            household_tracker.update(num_record=0, starts=household[h_psts], demands=household[s_demand], penalty=0)
            household[k_tracker] = household_tracker

            if inconvenience_cost_weight is not None:
                household[h_incon_weight] = inconvenience_cost_weight

            if num_dependent_tasks is not None:
                num_intervals = len(household[s_demand])
                durations = household[h_durs]
                num_total_tasks = len(durations)
                preferred_starts = household[h_psts]
                earliest_starts = household[h_ests]
                latest_ends = household[h_lfs]
                no_precedences, precedors, succ_delays \
                    = household_generation.new_dependent_tasks(
                    num_intervals=num_intervals, num_tasks_dependent=num_dependent_tasks,
                    ensure_dependent=ensure_dependent,
                    num_total_tasks=num_total_tasks,
                    preferred_starts=preferred_starts, durations=durations, earliest_starts=earliest_starts,
                    latest_ends=latest_ends)
                household[h_no_precs] = no_precedences
                household[h_precs] = precedors.copy()
                household[h_succ_delay] = succ_delays.copy()

            households[key] = household.copy()

        return households, preferred_demand_profile

    def __schedule_multiple_processing(self, households, prices, scheduling_method, model, solver, search,
                                       num_cpus=None, timeout=time_out, print_done=False):
        # parameter order: prices, scheduling_method, household, num_intervals, model, solver, search
        # if num_cpus is not None:
        #     pool = Pool(num_cpus)
        # else:
        #     pool = Pool()
        # results = pool.starmap_async(Household.schedule_household,
        #                              [(Household(), prices, scheduling_method, household,
        #                                self.num_intervals, model, solver, search, timeout, print_done)
        #                               for household in households.values()]).get()
        # pool.close()
        # pool.join()

        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = {executor.submit(Household.schedule_household, Household(), prices, scheduling_method, household,
                                       self.num_intervals, model, solver, search, timeout, print_done):
                           household for household in households.values()}

        return results

    def __retrieve_scheduling_results(self, results, num_iteration):
        aggregate_demand_profile = [0] * self.num_intervals
        total_weighted_inconvenience = 0
        time_scheduling_iteration = 0
        total_demand = 0

        for item in concurrent.futures.as_completed(results):
            res = item.result()

            # for res in results:
            # print(res)
            key = res[h_key]
            demands_household = res[s_demand]
            weighted_penalty_household = res[s_penalty]
            household_start_times = res[s_starts]
            time_household = res[t_time]
            total_demand += sum(demands_household)

            aggregate_demand_profile = [x + y for x, y in zip(demands_household, aggregate_demand_profile)]
            total_weighted_inconvenience += weighted_penalty_household
            time_scheduling_iteration += time_household

            # update each household's tracker
            self.households[key][k_tracker].update(num_record=num_iteration,
                                                   starts=household_start_times,
                                                   demands=demands_household,
                                                   penalty=weighted_penalty_household)

        return aggregate_demand_profile, total_weighted_inconvenience, time_scheduling_iteration
