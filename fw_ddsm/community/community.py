from multiprocessing import Pool, cpu_count
from numpy import genfromtxt
import pickle
from fw_ddsm.household.household import *
from fw_ddsm.cfunctions import average


class Community:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)

    def read(self, read_from_file, inconvenience_cost_weight=None):
        read_from_file = read_from_file if read_from_file.endswith("/") \
            else read_from_file + "/"
        self.households, self.aggregate_data = self.__existing_households(file_path=read_from_file,
                                                                          inconvenience_cost_weight=inconvenience_cost_weight)
        self.num_households = len(self.households)

    def new(self, file_probability_path, file_demand_list_path, algorithms_options,
            num_households=no_households,
            max_demand_multiplier=maxium_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_file_path=None):

        self.num_households = num_households

        self.households, self.aggregate_data, self.preferred_demand_profile \
            = self.__new_households(file_probability_path,
                                    file_demand_list_path,
                                    algorithms_options,
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
                                    write_to_file_path=write_to_file_path)

    def update_household(self, key, num_iteration, scheduling_method, demands=None, starts=None, penalty=None):

        if demands is not None:
            self.households[key][scheduling_method][k0_demand][num_iteration] = demands
            self.households[key][scheduling_method][k0_demand_max][num_iteration] = max(demands)
            # self.households[key][scheduling_method][k0_demand_total][num_iteration] = sum(demands)
            # self.households[key][scheduling_method][k0_par][num_iteration] = max(demands) / average(demands)
        if penalty is not None:
            self.households[key][scheduling_method][k0_penalty][num_iteration] = penalty
        # if time is not None:
        #     self.households[key][scheduling_method][k0_time][num_iteration] = time
        if starts is not None:
            self.households[key][scheduling_method][k0_starts][num_iteration] = starts

    def update_aggregate_data(self, num_iteration, scheduling_method, demands=None, prices=None, penalty=None, time=None):

        if demands is not None:
            self.aggregate_data[scheduling_method][k0_demand][num_iteration] = demands
            self.aggregate_data[scheduling_method][k0_demand_max][num_iteration] = max(demands)
            self.aggregate_data[scheduling_method][k0_demand_total][num_iteration] = sum(demands)
            self.aggregate_data[scheduling_method][k0_par][num_iteration] = max(demands) / average(demands)
        if penalty is not None:
            self.aggregate_data[scheduling_method][k0_penalty][num_iteration] = penalty
        if time is not None:
            self.aggregate_data[scheduling_method][k0_time][num_iteration] = time
        if prices is not None:
            self.aggregate_data[scheduling_method][k0_prices][num_iteration] = prices

    def schedule_all(self, num_iteration, prices, scheduling_method, model=None, solver=None, search=None):
        num_periods = len(prices)
        num_intervals_period = int(self.num_intervals / num_periods)
        if num_periods != self.num_intervals:
            prices = [p for p in prices for _ in range(num_intervals_period)]
        else:
            prices = [p for p in prices]
        self.update_aggregate_data(num_iteration=num_iteration, scheduling_method=scheduling_method, prices=prices)

        households = self.households
        print(f"Start scheduling households at iteration {num_iteration} using {scheduling_method}...")
        pool = Pool(cpu_count())
        results = pool.starmap_async(self.schedule_household,
                                     [(household, prices, scheduling_method, model, solver, search)
                                      for household in households.values()]).get()
        pool.close()
        pool.join()

        num_intervals = self.num_intervals
        aggregate_demand_profile = [0] * num_intervals
        total_inconvenience = 0
        time_scheduling_iteration = 0
        for res in results:
            key = res[h_key]
            demands_household = res[k0_demand]
            penalty_household = res[k0_penalty]
            time_household = res[k0_time]
            self.update_household(key=key, num_iteration=num_iteration, starts=res[k0_starts],
                                  penalty=penalty_household, demands=demands_household,
                                  scheduling_method=scheduling_method)
            aggregate_demand_profile = [x + y for x, y in zip(demands_household, aggregate_demand_profile)]
            total_inconvenience += penalty_household
            time_scheduling_iteration += time_household

        return aggregate_demand_profile, total_inconvenience, time_scheduling_iteration

    def schedule_household(self, household, prices, scheduling_method, model, solver, search):
        existing_household = Household()
        result = existing_household.schedule(prices=prices, scheduling_method=scheduling_method,
                                             household=household, model=model, solver=solver, search=search)
        return result

    def __new_households(self, file_probability_path, file_demand_list_path, algorithms_options,
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

        for h in range(num_households):
            new_household = Household()
            new_household.new(preferred_demand_profile,
                              list_of_devices_power,
                              algorithms_options,
                              max_demand_multiplier=max_demand_multiplier,
                              num_tasks_dependent=num_tasks_dependent,
                              full_flex_task_min=full_flex_task_min,
                              full_flex_task_max=full_flex_task_max,
                              semi_flex_task_min=semi_flex_task_min,
                              semi_flex_task_max=semi_flex_task_max,
                              fixed_task_min=fixed_task_min,
                              fixed_task_max=fixed_task_max,
                              inconvenience_cost_weight=inconvenience_cost_weight,
                              max_care_factor=max_care_factor, id=h)
            household_profile = new_household.data[h_demand_profile]
            aggregate_demand_profile = [x + y for x, y in zip(household_profile, aggregate_demand_profile)]
            households[h] = new_household.data.copy()

        # create aggregate trackers
        max_demand = max(aggregate_demand_profile)
        total_demand = sum(aggregate_demand_profile)
        par = max_demand / average(aggregate_demand_profile)
        aggregate_data = dict()
        for algorithm in algorithms_options.values():
            for alg in algorithm.values():
                if "fw" not in alg:
                    aggregate_data[alg] = dict()
                    aggregate_data[alg][k0_demand] = dict()
                    aggregate_data[alg][k0_demand_max] = dict()
                    aggregate_data[alg][k0_demand_total] = dict()
                    aggregate_data[alg][k0_par] = dict()
                    aggregate_data[alg][k0_penalty] = dict()
                    aggregate_data[alg][k0_final] = dict()
                    aggregate_data[alg][k0_prices] = dict()
                    aggregate_data[alg][k0_cost] = dict()
                    aggregate_data[alg][k0_time] = dict()

                    aggregate_data[alg][k0_demand][0] = aggregate_demand_profile
                    aggregate_data[alg][k0_demand_max][0] = max_demand
                    aggregate_data[alg][k0_demand_total][0] = total_demand
                    aggregate_data[alg][k0_par][0] = par
                    aggregate_data[alg][k0_par][0] = par
                    aggregate_data[alg][k0_penalty][0] = 0
                    aggregate_data[alg][k0_cost][0] = None
                    aggregate_data[alg][k0_time][0] = 0

        # write household data and area data into files
        if write_to_file_path is not None:
            write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
                else write_to_file_path + "/"
            path = Path(write_to_file_path)
            if not path.exists():
                path.mkdir(mode=0o777, parents=True, exist_ok=False)

            with open(f"{write_to_file_path}{file_community_pkl}", 'wb+') as f:
                pickle.dump(households, f, pickle.HIGHEST_PROTOCOL)
            f.close()

            with open(f"{write_to_file_path}{file_community_meta_pkl}", 'wb+') as f:
                pickle.dump(aggregate_data, f, pickle.HIGHEST_PROTOCOL)
            f.close()

        return households, aggregate_data, aggregate_demand_profile

    def __existing_households(self, file_path, inconvenience_cost_weight=None):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #

        with open(f"{file_path}{file_community_pkl}", 'rb') as f:
            households = pickle.load(f)
        f.close()

        with open(f"{file_path}{file_community_meta_pkl}", 'rb') as f:
            households_meta = pickle.load(f)
        f.close()

        if inconvenience_cost_weight is not None:
            for household in households.values():
                household["care_factor_weight"] = inconvenience_cost_weight

        return households, households_meta
