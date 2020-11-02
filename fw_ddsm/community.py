from multiprocessing import Pool, cpu_count, freeze_support
import pickle
from fw_ddsm.cfunctions import average
from fw_ddsm.household import *
from fw_ddsm.tracker import *


class Community:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)
        self.num_households = 0
        self.households = dict()
        self.community_tracker = Tracker()
        self.community_final = Tracker()
        self.scheduling_method = ""
        self.preferred_demand_profile = []

    def read(self, scheduling_method, read_from_folder, inconvenience_cost_weight=None):
        read_from_folder = read_from_folder if read_from_folder.endswith("/") \
            else read_from_folder + "/"

        self.households = dict()
        self.community_tracker = Tracker()
        self.community_final = Tracker()
        self.preferred_demand_profile = None

        self.households = self.__existing_households(file_path=read_from_folder,
                                                     inconvenience_cost_weight=inconvenience_cost_weight)
        self.preferred_demand_profile = self.households.pop(k0_demand)
        self.num_households = len(self.households) - 1
        self.community_tracker.new(method=scheduling_method)
        self.community_tracker.update(num_record=0, demands=self.preferred_demand_profile, penalty=0)
        self.community_final.new(method=scheduling_method)
        print("The community is read. ")

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
        self.community_tracker = Tracker()
        self.community_final = Tracker()
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
        self.community_tracker.new(method=scheduling_method)
        self.community_tracker.update(num_record=0, demands=self.preferred_demand_profile, penalty=0)
        self.community_final.new(method=scheduling_method)

        if write_to_file_path is not None:
            self.write_to_file(write_to_file_path)

        print("The community is created. ")

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
        prices = self.__convert_price(prices)
        self.community_tracker.update(num_record=num_iteration, prices=prices)

        print(f"{num_iteration}. Start scheduling households using {scheduling_method}...")
        if households is None:
            households = self.households
        results = self.__schedule_multiple_processing(households=households, prices=prices,
                                                      scheduling_method=scheduling_method,
                                                      model=model, solver=solver, search=search)

        aggregate_demand_profile, total_inconvenience, time_scheduling_iteration \
            = self.__retrieve_scheduling_results(results=results, num_iteration=num_iteration)

        return aggregate_demand_profile, total_inconvenience, time_scheduling_iteration

    def decide_final_schedules(self, probability_distribution, num_sample=0):
        existing_household = Household()
        final_aggregate_demand_profile = [0] * self.num_intervals
        final_total_inconvenience = 0
        for household in self.households.values():
            chosen_demand_profile, chosen_penalty \
                = existing_household.decide_final_schedule(household_tracker=household[k0_tracker],
                                                           scheduling_method=self.scheduling_method,
                                                           probability_distribution=probability_distribution)
            final_aggregate_demand_profile = [x + y for x, y in
                                              zip(chosen_demand_profile, final_aggregate_demand_profile)]
            final_total_inconvenience += chosen_penalty

        self.community_final.update(num_record=num_sample, demands=final_aggregate_demand_profile,
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

    def __new_households(self, file_probability_path, file_demand_list_path,
                         max_demand_multiplier=maxium_demand_multiplier,
                         num_tasks_dependent=no_tasks_dependent,
                         full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
                         semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
                         fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
                         inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
                         write_to_file_path=None):
        # ---------------------------------------------------------------------- #

        # ---------------------------------------------------------------------- #

        num_households = self.num_households
        preferred_demand_profile = genfromtxt(file_probability_path, delimiter=',', dtype="float")
        list_of_devices_power = genfromtxt(file_demand_list_path, delimiter=',', dtype="float")

        households = dict()
        num_intervals = self.num_intervals
        aggregate_demand_profile = [0] * num_intervals

        new_household = Household()
        for h in range(num_households):
            new_household.new(scheduling_method=self.scheduling_method,
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
            household_profile = new_household.tasks[k0_demand]
            aggregate_demand_profile = [x + y for x, y in zip(household_profile, aggregate_demand_profile)]

            households[h] = new_household.tasks.copy()
            households[h][k0_tracker] = new_household.household_tracker.data

        del new_household

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

        pool = Pool(cpu_count())
        single_household = Household()
        results = pool.starmap_async(single_household.schedule,
                                     [(prices, scheduling_method, self.num_intervals, household, model, solver, search)
                                      for household in households.values()]).get()
        # results = pool.starmap_async(self.schedule_single_household,
        #                              [(household, prices, scheduling_method, model, solver, search)
        #                               for household in households.values()]).get()
        # parameters' order: prices, scheduling_method, num_intervals=None, household = None,
        # model = None, solver = None, search = None
        pool.close()
        pool.join()
        return results

    def __retrieve_scheduling_results(self, results, num_iteration):
        aggregate_demand_profile = [0] * self.num_intervals
        total_inconvenience = 0
        time_scheduling_iteration = 0
        tasks_tracker = Tracker()
        for res in results:
            key = res[h_key]
            demands_household = res[k0_demand]
            penalty_household = res[k0_penalty]
            time_household = res[k0_time]

            aggregate_demand_profile = [x + y for x, y in zip(demands_household, aggregate_demand_profile)]
            total_inconvenience += penalty_household
            time_scheduling_iteration += time_household

            # update each household's tracker
            tasks_tracker.update(num_record=num_iteration, tracker=self.households[key][k0_tracker],
                                 method=self.scheduling_method, demands=demands_household, penalty=penalty_household)

        return aggregate_demand_profile, total_inconvenience, time_scheduling_iteration
