from multiprocessing import Pool, cpu_count
import pickle
from time import time
from fw_ddsm.household import *
from fw_ddsm.tracker import *
from scripts import household_generation, household_scheduling


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
        if not read_from_folder.endswith("/"):
            read_from_folder += "/"

        self.households = dict()
        self.households, self.preferred_demand_profile \
            = self.__existing_households(file_path=read_from_folder,
                                         inconvenience_cost_weight=inconvenience_cost_weight)
        if k0_demand in self.households:
            self.num_households = len(self.households) - 1
        self.new_community_tracker(scheduling_method=scheduling_method)
        print("0. The community is read. ")

        return self.preferred_demand_profile

    def new(self, file_preferred_demand_profile, file_demand_list, scheduling_method,
            num_intervals=no_intervals, num_households=no_households,
            max_demand_multiplier=maxium_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_file_path=None):

        self.scheduling_method = scheduling_method
        self.num_intervals = num_intervals
        self.num_households = num_households

        households = dict()
        aggregate_demand_profile = [0] * num_intervals
        preferred_demand_profile = genfromtxt(file_preferred_demand_profile, delimiter=',', dtype="float")
        list_of_devices_power = genfromtxt(file_demand_list, delimiter=',', dtype="float")
        for h in range(num_households):
            household, household_demand_profile \
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
                                                     household_id=h)
            aggregate_demand_profile = [x + y for x, y in zip(household_demand_profile, aggregate_demand_profile)]

            households[h] = household.copy()
            households[h][k0_tracker] = Tracker()
            households[h][k0_tracker].new()
            households[h][k0_tracker].update(num_record=0, demands=household_demand_profile, penalty=0)

        self.households = households
        self.preferred_demand_profile = aggregate_demand_profile
        self.new_community_tracker(scheduling_method=scheduling_method)

        self.write_to_file("data/")
        if write_to_file_path is not None:
            self.write_to_file(write_to_file_path)

        print("0. The community is created. ")
        return aggregate_demand_profile

    def new_community_tracker(self, scheduling_method):
        self.tracker = Tracker()
        self.tracker.new(name=f"{scheduling_method}_community")
        self.tracker.update(num_record=0, penalty=0, run_time=0)
        self.final = Tracker()
        self.final.new(name=f"{scheduling_method}_community_final")
        self.tracker.update(num_record=0, penalty=0, run_time=0)

    def write_to_file(self, folder):

        if not folder.endswith("/"):
            folder += "/"
        path = Path(folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        self.households[k0_demand] = self.preferred_demand_profile
        with open(f"{folder}{file_community_pkl}", 'wb+') as f:
            pickle.dump(self.households, f, pickle.HIGHEST_PROTOCOL)
        del self.households[k0_demand]
        f.close()

    def schedule(self, num_iteration, prices, scheduling_method, model=None, solver=None, search=None, households=None):

        prices = self.__convert_price(prices)

        if households is None:
            households = self.households
        results = self.__schedule_multiple_processing(households=households, prices=prices,
                                                      scheduling_method=scheduling_method,
                                                      model=model, solver=solver, search=search)

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
            chosen_demand_profile, chosen_penalty \
                = Household.finalise_household(self=Household(), household_tracker_data=household[k0_tracker].data,
                                               probability_distribution=start_probability_distribution)
            final_aggregate_demand_profile \
                = [x + y for x, y in zip(chosen_demand_profile, final_aggregate_demand_profile)]
            final_total_inconvenience += chosen_penalty
            total_demand += sum(chosen_demand_profile)

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

    def __existing_households(self, file_path, inconvenience_cost_weight=None):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #
        with open(f"{file_path}{file_community_pkl}", 'rb') as f:
            households = pickle.load(f)
        f.close()
        preferred_demand_profile = households.pop(k0_demand)

        for household in households.values():
            household_tracker = Tracker()
            household_tracker.new()
            household_tracker.update(num_record=0, demands=household[k0_demand], penalty=0)
            household[k0_tracker] = household_tracker

            if inconvenience_cost_weight is not None:
                household["care_factor_weight"] = inconvenience_cost_weight

        return households, preferred_demand_profile

    def __schedule_multiple_processing(self, households, prices, scheduling_method, model, solver, search):
        pool = Pool(cpu_count())
        results = pool.starmap_async(Household.schedule_household,
                                     [(Household(), prices, scheduling_method, household,
                                       self.num_intervals, model, solver, search)
                                      for household in households.values()]).get()
        # parameter order: prices, scheduling_method, household, num_intervals, model, solver, search
        pool.close()
        pool.join()
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
            self.households[key][k0_tracker].update(num_record=num_iteration, demands=demands_household,
                                                    penalty=weighted_penalty_household)

        return aggregate_demand_profile, total_weighted_inconvenience, time_scheduling_iteration
