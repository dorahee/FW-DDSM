from multiprocessing import Pool, cpu_count
import pickle
from time import time
from fw_ddsm.household import *
from fw_ddsm.tracker import *


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

    def read(self, scheduling_method, read_from_folder="data/", inconvenience_cost_weight=None):
        read_from_folder = read_from_folder if read_from_folder.endswith("/") \
            else read_from_folder + "/"

        self.households = dict()
        self.preferred_demand_profile = None

        self.households = self.__existing_households(file_path=read_from_folder,
                                                     inconvenience_cost_weight=inconvenience_cost_weight)
        self.preferred_demand_profile = self.households.pop(k0_demand)
        self.num_households = len(self.households) - 1

        self.tracker.new(method=scheduling_method)
        self.tracker.update(num_record=0, method=scheduling_method,
                            demands=self.preferred_demand_profile, penalty=0)
        self.final.new(method=scheduling_method)
        print("0. The community is read. ")

    def new(self, file_preferred_demand_profile_path, file_demand_list_path, scheduling_method,
            num_households=no_households,
            max_demand_multiplier=maxium_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_file_path=None):

        self.households = dict()
        self.preferred_demand_profile = None

        self.scheduling_method = scheduling_method
        self.num_households = num_households
        self.households, self.preferred_demand_profile \
            = self.__new_households(file_preferred_demand_profile_path,
                                    file_demand_list_path,
                                    max_demand_multiplier=max_demand_multiplier,
                                    num_tasks_dependent=num_tasks_dependent,
                                    full_flex_task_min=full_flex_task_min,
                                    full_flex_task_max=full_flex_task_max,
                                    semi_flex_task_min=semi_flex_task_min,
                                    semi_flex_task_max=semi_flex_task_max,
                                    fixed_task_min=fixed_task_min,
                                    fixed_task_max=fixed_task_max,
                                    inconvenience_cost_weight=inconvenience_cost_weight,
                                    max_care_factor=max_care_factor)
        self.tracker.new(method=scheduling_method)
        self.tracker.update(num_record=0, method=scheduling_method, penalty=0,
                            run_time=0)
        self.final.new(method=scheduling_method)
        self.tracker.update(num_record=0, method=scheduling_method, penalty=0, run_time=0)

        self.write_to_file("data/")
        if write_to_file_path is not None:
            self.write_to_file(write_to_file_path)

        print("0. The community is created. ")

    def write_to_file(self, write_to_file_path):

        write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
            else write_to_file_path + "/"
        path = Path(write_to_file_path)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        self.households[k0_demand] = self.preferred_demand_profile
        with open(f"{write_to_file_path}{file_community_pkl}", 'wb+') as f:
            pickle.dump(self.households, f, pickle.HIGHEST_PROTOCOL)
        del self.households[k0_demand]
        f.close()

    def schedule(self, num_iteration, prices, scheduling_method, model=None, solver=None, search=None, households=None):
        print(f"{num_iteration}. Scheduling {self.num_households} households, {scheduling_method}...")

        prices = self.__convert_price(prices)
        # self.tracker.update(num_record=num_iteration - 1, method=scheduling_method, prices=prices)

        if households is None:
            households = self.households
        results = self.__schedule_multiple_processing(households=households, prices=prices,
                                                      scheduling_method=scheduling_method,
                                                      model=model, solver=solver, search=search)

        aggregate_demand_profile, weighted_total_inconvenience, time_scheduling_iteration \
            = self.__retrieve_scheduling_results(results=results, num_iteration=num_iteration)

        self.tracker.update(num_record=num_iteration, method=scheduling_method,
                            penalty=weighted_total_inconvenience, run_time=time_scheduling_iteration)

        return aggregate_demand_profile, weighted_total_inconvenience, time_scheduling_iteration

    def decide_final_schedules(self, start_probability_distribution, num_sample=0):
        final_aggregate_demand_profile = [0] * self.num_intervals
        final_total_inconvenience = 0
        total_demand = 0
        for household in self.households.values():
            chosen_demand_profile, chosen_penalty \
                = finalise_schedule(household_tracker_data=household[k0_tracker],
                                    household_final=None,
                                    scheduling_method=self.scheduling_method,
                                    probability_distribution=start_probability_distribution)
            final_aggregate_demand_profile = [x + y for x, y in
                                              zip(chosen_demand_profile, final_aggregate_demand_profile)]
            final_total_inconvenience += chosen_penalty
            total_demand += sum(chosen_demand_profile)

        # self.final.update(num_record=num_sample, method=self.scheduling_method,
        #                   demands=final_aggregate_demand_profile, penalty=final_total_inconvenience)

        return final_aggregate_demand_profile, final_total_inconvenience

    def __convert_price(self, prices):
        num_periods = len(prices)
        num_intervals_period = int(self.num_intervals / num_periods)
        if num_periods != self.num_intervals:
            prices = [p for p in prices for _ in range(num_intervals_period)]
        else:
            prices = [p for p in prices]

        return prices

    def __new_households(self, file_probability_path, file_demand_list_path,
                         max_demand_multiplier=maxium_demand_multiplier,
                         num_tasks_dependent=no_tasks_dependent,
                         full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
                         semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
                         fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
                         inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max):
        # ---------------------------------------------------------------------- #

        # ---------------------------------------------------------------------- #

        num_households = self.num_households
        preferred_demand_profile = genfromtxt(file_probability_path, delimiter=',', dtype="float")
        list_of_devices_power = genfromtxt(file_demand_list_path, delimiter=',', dtype="float")

        households = dict()
        num_intervals = self.num_intervals
        aggregate_demand_profile = [0] * num_intervals

        for h in range(num_households):
            tasks, household_tracker = new_household(
                num_intervals=self.num_intervals,
                scheduling_method=self.scheduling_method,
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
                max_care_factor=max_care_factor, household_id=h)
            household_profile = tasks[k0_demand]
            aggregate_demand_profile = [x + y for x, y in zip(household_profile, aggregate_demand_profile)]

            households[h] = tasks.copy()
            households[h][k0_tracker] = household_tracker.data.copy()

        return households, aggregate_demand_profile

    def __existing_households(self, file_path, inconvenience_cost_weight=None):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #

        with open(f"{file_path}{file_community_pkl}", 'rb') as f:
            households = pickle.load(f)
        f.close()

        if inconvenience_cost_weight is not None:
            for household in households.values():
                household["care_factor_weight"] = inconvenience_cost_weight

        return households

    def __schedule_multiple_processing(self, households, prices, scheduling_method, model, solver, search):

        t_begin = time()
        pool = Pool(cpu_count())
        results = pool.starmap_async(schedule_household,
                                     [(prices, scheduling_method, household,
                                       self.num_intervals, model, solver, search)
                                      for household in households.values()]).get()
        # parameters' order: prices, scheduling_method = self.scheduling_method, household = self.tasks, num_intervals = self.num_intervals, model = model, solver = solver, search = search
        # results = pool.starmap_async(self.schedule_single_household,
        #                              [(household, prices, scheduling_method, model, solver, search)
        #                               for household in households.values()]).get()
        pool.close()
        pool.join()
        print(f"   Finish scheduling in {round(time() - t_begin)} seconds. ")
        return results

    def __retrieve_scheduling_results(self, results, num_iteration):
        aggregate_demand_profile = [0] * self.num_intervals
        total_weighted_inconvenience = 0
        time_scheduling_iteration = 0
        total_demand = 0
        for res in results:
            key = res[h_key]
            demands_household = res[k0_demand]
            weighted_penalty_household = res[k0_penalty]
            time_household = res[k0_time]
            total_demand += sum(demands_household)

            aggregate_demand_profile = [x + y for x, y in zip(demands_household, aggregate_demand_profile)]
            total_weighted_inconvenience += weighted_penalty_household
            time_scheduling_iteration += time_household

            # update each household's tracker
            self.households[key][k0_tracker] \
                = Tracker.update(self=Tracker(), num_record=num_iteration, tracker=self.households[key][k0_tracker],
                                 method=self.scheduling_method, demands=demands_household,
                                 penalty=weighted_penalty_household).copy()

        return aggregate_demand_profile, total_weighted_inconvenience, time_scheduling_iteration
