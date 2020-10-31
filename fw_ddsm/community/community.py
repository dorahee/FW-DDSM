from fw_ddsm.household.household import *
from multiprocessing import Pool



class Community:

    def __init__(self, num_households=no_households, num_intervals=no_intervals, num_periods=no_periods):
        self.data = dict()
        self.num_households = num_households
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)
        self.preferred_demand_profile = []

    def Community(self):
        return 0

    def read(self, file_path, inconvenience_cost_weight=None):
        self.data = self.__existing_households(file_path, inconvenience_cost_weight)

    def new(self, file_probability_path, file_demand_list_path, algorithms_options,
            max_demand_multiplier=maxium_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_file_path=None):

        self.data, self.preferred_demand_profile \
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
                              max_care_factor=max_care_factor,
                              write_to_file_path=write_to_file_path, id=h)
            household_profile = new_household.data[h_demand_profile]
            aggregate_demand_profile = [x + y for x, y in zip(household_profile, aggregate_demand_profile)]
            households[h] = new_household.data.copy()

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

        return households, aggregate_demand_profile

    def __existing_households(self, file_path, inconvenience_cost_weight=None):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #

        file_path = file_path if file_path.endswith("/") else file_path + "/"

        with open(file_path + "households" + '.pkl', 'rb') as f:
            households = pickle.load(f)
        f.close()

        if inconvenience_cost_weight is not None:
            for household in households.values():
                household["care_factor_weight"] = inconvenience_cost_weight

        return households


    def __schedule_household(self, household, prices, scheduling_method, model, solver, search):
        existing_household = Household()
        existing_household.read(existing_household=household)
        result = existing_household.schedule(prices, scheduling_method,
                                             model=model, solver=solver, search=search)
        return result


    def schedule(self, num_iteration, prices, scheduling_method, model, solver, search):
        print("Start scheduling households...")
        households = self.data
        pool = Pool()
        results = pool.starmap_async(self.__schedule_household,
                                     [(household, prices, scheduling_method, model, solver, search)
                                      for household in households.values()]).get()
        pool.close()
        pool.join()

        num_intervals = self.num_intervals
        aggregate_demand_profile = [0] * num_intervals
        total_inconvenience_cost = 0
        time_scheduling_iteration = 0
        for res in results:
            key = res[h_key]
            self.data[key][k0_starts][scheduling_method][num_iteration] = res[k0_starts]
            self.data[key][k0_penalty][scheduling_method][num_iteration] = res[k0_penalty]
            self.data[key][k0_demand][scheduling_method][num_iteration] = res[k0_demand]

            aggregate_demand_profile = [x + y for x, y in zip(res[k0_demand], aggregate_demand_profile)]
            total_inconvenience_cost += res[k0_penalty]
            time_scheduling_iteration += res[k0_time]

        return aggregate_demand_profile, total_inconvenience_cost, time_scheduling_iteration
