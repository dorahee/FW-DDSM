from multiprocessing import Pool, cpu_count
import concurrent.futures
import pickle
from time import time
from fw_ddsm.household import *
from fw_ddsm.tracker import *
from fw_ddsm.scripts import household_generation, aggregator_pricing


class Community:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.num_intervals = num_intervals
        self.num_periods = num_periods
        self.num_intervals_periods = int(num_intervals / num_periods)
        self.num_households = 0
        self.community_details = dict()
        self.tracker = Tracker()
        self.final = Tracker()
        self.tasks_scheduling_method = ""
        self.preferred_demand_profile = []

    def read(self, tasks_scheduling_method,
             read_from_folder="data/",
             inconvenience_cost_weight=None,
             num_dependent_tasks=None, ensure_dependent=False,
             date_time=None):
        if not read_from_folder.endswith("/"):
            read_from_folder += "/"
        # read_from_folder += "data/"

        # read the community details
        self.community_details = dict()
        self.community_details, self.preferred_demand_profile \
            = self.__existing_households(file_path=read_from_folder, date_time=date_time,
                                         inconvenience_cost_weight=inconvenience_cost_weight,
                                         num_dependent_tasks=num_dependent_tasks, ensure_dependent=ensure_dependent)

        # read the number of households in this community
        if s_demand in self.community_details:
            self.num_households = len(self.community_details) - 1

        # prices, total_cost \
        #     = aggregator_pricing.prices_and_cost(aggregate_demand_profile=self.preferred_demand_profile,
        #                                          pricing_table=pricing_table,
        #                                          cost_function=cost_function_type)

        # generate a new tracker for the results at each iteration
        self.new_community_tracker(tasks_scheduling_method=tasks_scheduling_method)
        self.tracker.update(num_record=0, demands=self.preferred_demand_profile)

        # print a message when done
        print("0. The community is read. ")

        return self.preferred_demand_profile

    def new(self, file_preferred_demand_profile, file_demand_list,
            tasks_scheduling_method,
            num_intervals=no_intervals, num_households=no_households,
            max_demand_multiplier=maximum_demand_multiplier,
            num_tasks_dependent=no_tasks_dependent, ensure_dependent=False,
            full_flex_task_min=no_full_flex_tasks_min, full_flex_task_max=0,
            semi_flex_task_min=no_semi_flex_tasks_min, semi_flex_task_max=0,
            fixed_task_min=no_fixed_tasks_min, fixed_task_max=0,
            inconvenience_cost_weight=care_f_weight, max_care_factor=care_f_max,
            write_to_file_path=None, backup_file_path=None, date_time=None,
            capacity_max=battery_capacity_max, capacity_min=battery_capacity_min,
            power=battery_power):

        self.tasks_scheduling_method = tasks_scheduling_method
        self.num_intervals = num_intervals
        self.num_households = num_households

        # generate details of households in this community
        community_details = dict()
        aggregate_demand_profile = [0] * num_intervals
        preferred_demand_profile = genfromtxt(file_preferred_demand_profile, delimiter=',', dtype="float")
        list_of_devices_power = genfromtxt(file_demand_list, delimiter=',', dtype="float")
        for h in range(num_households):
            household_details \
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
                                                     household_id=h,
                                                     capacity_max=capacity_max,
                                                     capacity_min=capacity_min,
                                                     power=power)

            # save the details of this household
            community_details[h] = household_details.copy()

            # update the aggregate demand profile
            household_demand_profile = household_details[s_demand].copy()
            aggregate_demand_profile = [x + y for x, y in zip(household_demand_profile, aggregate_demand_profile)]

            # generate a tracker for each household to record the results at each iteration
            community_details[h][k_tracker] = Tracker()
            community_details[h][k_tracker].new()
            community_details[h][k_tracker].update(num_record=0, tasks_starts=household_details[h_psts],
                                                   demands=household_demand_profile, penalty=0,
                                                   battery_profile=household_details[b_profile])

        # save the initial details
        self.community_details = community_details
        self.preferred_demand_profile = aggregate_demand_profile

        # write the new community details to a file if needed
        if write_to_file_path is not None:
            self.save_to_file(write_to_file_path, date_time=date_time)
        else:
            self.save_to_file("data/")

        # backup the new community details to a folder if needed
        if backup_file_path is not None:
            self.save_to_file(backup_file_path, date_time=date_time)

        # generate a new tracker for the aggregate results of the community at each iteration
        # prices, total_cost \
        #     = aggregator_pricing.prices_and_cost(aggregate_demand_profile=self.preferred_demand_profile,
        #                                          pricing_table=pricing_table,
        #                                          cost_function=cost_function_type)
        self.new_community_tracker(tasks_scheduling_method=tasks_scheduling_method)
        self.tracker.update(num_record=0, demands=aggregate_demand_profile)

        # print a message when done
        print("0. The community is created. ")

        return aggregate_demand_profile

    def new_community_tracker(self, tasks_scheduling_method):
        self.tracker = Tracker()
        self.tracker.new(name=f"{tasks_scheduling_method}_community")
        self.tracker.update(num_record=0, penalty=0, run_time=0)
        self.final = Tracker()
        self.final.new(name=f"{tasks_scheduling_method}_community_final")
        self.tracker.update(num_record=0, penalty=0, run_time=0)

    def save_to_file(self, folder="data/", date_time=None):

        if not folder.endswith("/"):
            folder += "/"
        folder += "data/"
        path = Path(folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        self.community_details[s_demand] = self.preferred_demand_profile
        if date_time is None:
            file_name = f"{folder}{file_community_pkl}"
        else:
            file_name = f"{folder}{date_time}_{file_community_pkl}"
        with open(file_name, 'wb+') as f:
            pickle.dump(self.community_details, f, pickle.HIGHEST_PROTOCOL)
        del self.community_details[s_demand]
        f.close()

    def schedule(self, num_iteration, prices, pricing_table,
                 tasks_scheduling_method,
                 community_details=None, num_intervals=None,
                 model=None, solver=None, search=None,
                 use_battery=False, battery_model=None, battery_solver=None,
                 num_cpus=None, timeout=time_out,
                 fully_charge_time=fully_charge_hour,
                 print_upon_completion=False):

        prices = self.__convert_price(prices)

        if community_details is None:
            community_details = self.community_details
        if num_intervals is None:
            num_intervals = no_intervals
        if use_battery:
            battery_model = file_mip_battery if battery_model is None else battery_model
            battery_solver = "mip" if battery_solver is None else battery_solver

        results \
            = self.__schedule_households(num_iteration=num_iteration,
                                         num_intervals=num_intervals,
                                         community_details=community_details,
                                         prices=prices,
                                         tasks_scheduling_method=tasks_scheduling_method,
                                         model=model, solver=solver, search=search,
                                         use_battery=use_battery,
                                         battery_model=battery_model,
                                         battery_solver=battery_solver,
                                         num_cpus=num_cpus, timeout=timeout,
                                         fully_charge_time=fully_charge_time,
                                         print_upon_completion=print_upon_completion)

        aggregate_demand_profile, aggregate_battery_profile, weighted_total_inconvenience, time_scheduling_iteration \
            = self.__retrieve_scheduling_results(results=results, num_iteration=num_iteration)

        prices, total_cost \
            = aggregator_pricing.prices_and_cost(aggregate_demand_profile=aggregate_demand_profile,
                                                 pricing_table=pricing_table,
                                                 cost_function=cost_function_type)
        obj = total_cost + weighted_total_inconvenience

        self.tracker.update(num_record=num_iteration, penalty=weighted_total_inconvenience,
                            cost=total_cost,
                            run_time=time_scheduling_iteration,
                            demands=aggregate_demand_profile)

        return aggregate_demand_profile, aggregate_battery_profile, \
               weighted_total_inconvenience, time_scheduling_iteration, obj

    def finalise_schedule(self, start_probability_distribution, tasks_scheduling_method=None, num_sample=0):
        final_aggregate_demand_profile = [0] * self.num_intervals
        final_battery_profile = [0] * self.num_intervals
        final_total_inconvenience = 0
        total_demand = 0
        for household in self.community_details.values():
            chosen_demand_profile, chosen_penalty, chosen_start_times, chosen_battery_profile \
                = Household.finalise_household(self=Household(), household_tracker_data=household[k_tracker].data,
                                               probability_distribution=start_probability_distribution)
            final_aggregate_demand_profile \
                = [x + y for x, y in zip(chosen_demand_profile, final_aggregate_demand_profile)]
            final_battery_profile \
                = [x + y for x, y in zip(chosen_battery_profile, final_battery_profile)]
            final_total_inconvenience += chosen_penalty
            total_demand += sum(chosen_demand_profile)

            household_id = household[h_key]
            if k_tracker_final not in self.community_details[household_id]:
                self.community_details[household_id][k_tracker_final] = Tracker()
                self.community_details[household_id][k_tracker_final].new()
            self.community_details[household_id][k_tracker_final].update(num_record=num_sample,
                                                                         tasks_starts=chosen_start_times,
                                                                         demands=chosen_demand_profile,
                                                                         penalty=chosen_penalty,
                                                                         battery_profile=chosen_battery_profile)

        self.final.update(num_record=num_sample, demands=final_aggregate_demand_profile,
                          battery_profile=final_battery_profile,
                          penalty=final_total_inconvenience)

        return final_aggregate_demand_profile, final_battery_profile, final_total_inconvenience

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

        # read the community details from the file
        with open(file_name, 'rb') as f:
            community_details = pickle.load(f)
        f.close()
        preferred_demand_profile = community_details.pop(s_demand)

        # read the details of each household
        for key, household_details in community_details.items():

            # create a tracker for each household to record the schedule/profile at each iteration
            household_tracker = Tracker()
            household_tracker.new()
            household_tracker.update(num_record=0, tasks_starts=household_details[h_psts],
                                     demands=household_details[s_demand],
                                     penalty=0,
                                     battery_profile=household_details[b_profile])
            household_details[k_tracker] = household_tracker

            # update the inconvenience cost weight if applicable
            if inconvenience_cost_weight is not None:
                household_details[h_incon_weight] = inconvenience_cost_weight

            # generate new dependent tasks for this household if applicable
            if num_dependent_tasks is not None:
                num_intervals = len(household_details[s_demand])
                durations = household_details[h_durs]
                num_total_tasks = len(durations)
                preferred_starts = household_details[h_psts]
                earliest_starts = household_details[h_ests]
                latest_ends = household_details[h_lfs]
                no_precedences, precedors, succ_delays \
                    = household_generation.new_dependent_tasks(
                    num_intervals=num_intervals, num_tasks_dependent=num_dependent_tasks,
                    ensure_dependent=ensure_dependent,
                    num_total_tasks=num_total_tasks,
                    preferred_starts=preferred_starts, durations=durations, earliest_starts=earliest_starts,
                    latest_ends=latest_ends)
                household_details[h_no_precs] = no_precedences
                household_details[h_precs] = precedors.copy()
                household_details[h_succ_delay] = succ_delays.copy()

            # save the existing household details
            community_details[key] = household_details.copy()

        # return the existing community details
        return community_details, preferred_demand_profile

    def __schedule_households(self, num_iteration, num_intervals,
                              community_details, prices,
                              tasks_scheduling_method, model, solver, search,
                              use_battery=False, battery_model=None, battery_solver=None,
                              num_cpus=None, timeout=time_out,
                              fully_charge_time=fully_charge_hour,
                              print_upon_completion=False):

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

        if use_battery:
            battery_model = file_mip_battery if battery_model is None else battery_model
            battery_solver = "mip" if battery_solver is None else battery_solver

        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = {
                executor.submit(Household.schedule, Household(),
                                num_iteration, prices,
                                num_intervals,
                                household_details,
                                tasks_scheduling_method,
                                model, solver, search,
                                use_battery, battery_model, battery_solver,
                                timeout, fully_charge_time,
                                False,
                                print_upon_completion):
                    household_details for household_details in community_details.values()}

        return results

    def __retrieve_scheduling_results(self, results, num_iteration):
        aggregate_demand_profile = [0] * self.num_intervals
        aggregate_battery_profile = [0] * self.num_intervals
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
            start_times_jobs = res[s_starts]
            battery_profile_household = res[b_profile]
            time_household = res[t_time]
            total_demand += sum(demands_household)

            aggregate_demand_profile = [x + y for x, y in zip(demands_household, aggregate_demand_profile)]
            aggregate_battery_profile = [x + y for x, y in zip(battery_profile_household, aggregate_battery_profile)]
            total_weighted_inconvenience += weighted_penalty_household
            time_scheduling_iteration += time_household

            # update each household's tracker
            self.community_details[key][k_tracker].update(num_record=num_iteration,
                                                          tasks_starts=start_times_jobs,
                                                          demands=demands_household,
                                                          penalty=weighted_penalty_household,
                                                          battery_profile=battery_profile_household)

        return aggregate_demand_profile, aggregate_battery_profile, \
               total_weighted_inconvenience, time_scheduling_iteration
