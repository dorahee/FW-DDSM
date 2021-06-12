from pandas import read_csv
import pickle
from math import ceil
from more_itertools import grouper
from time import time
from pathlib import Path
from fw_ddsm.tracker import *
from fw_ddsm.scripts import aggregator_generation, aggregator_pricing


class Aggregator:

    def __init__(self, num_periods=no_periods, cost_function="piece-wise"):
        self.num_periods = num_periods
        self.cost_function_type = cost_function
        self.pricing_table = dict()
        self.pricing_method = ""

        self.tracker = Tracker()
        self.final = Tracker()

        self.start_time_probability = None
        self.init_demand_max = 0
        self.init_cost = 0
        self.preferred_demand_profile = []

    def read_aggregator(self, pricing_method, aggregate_preferred_demand_profile,
                        read_from_folder="data/", date_time=None):
        self.pricing_table = dict()
        self.pricing_method = pricing_method
        aggregate_preferred_demand_profile = self.__convert_demand_profile(aggregate_preferred_demand_profile)
        self.preferred_demand_profile = aggregate_preferred_demand_profile

        read_from_folder = read_from_folder if read_from_folder.endswith("/") \
            else read_from_folder + "/"
        self.pricing_table = self.__existing_pricing_table(read_from_folder, date_time=date_time)
        self.new_aggregator_tracker(pricing_method=pricing_method,
                                    aggregate_preferred_demand_profile=aggregate_preferred_demand_profile)
        print("0. Aggregator is read. ")

        prices, consumption_cost, inconvenience, step, \
        new_aggregate_demand_profile, new_aggregate_battery_profile,time_pricing \
            = self.pricing(num_iteration=0,
                           aggregate_demand_profile=aggregate_preferred_demand_profile,
                           aggregate_battery_profile=[0] * len(aggregate_preferred_demand_profile),
                           aggregate_inconvenience=0)
        return prices, consumption_cost

    def new_aggregator(self, normalised_pricing_table_csv, aggregate_preferred_demand_profile, pricing_method,
                       max_scale=0, num_periods=no_periods, weight=pricing_table_weight,
                       write_to_file_path=None, backup_file_path=None,
                       date_time=None):
        self.pricing_table = dict()
        self.pricing_method = pricing_method

        aggregate_preferred_demand_profile = self.__convert_demand_profile(aggregate_preferred_demand_profile)
        self.preferred_demand_profile = aggregate_preferred_demand_profile
        maximum_demand_level = max(aggregate_preferred_demand_profile) if max_scale == 0 else max_scale
        self.pricing_table = aggregator_generation.new_pricing_table(
            normalised_pricing_table_csv=normalised_pricing_table_csv,
            maximum_demand_level=maximum_demand_level, weight=weight, num_periods=num_periods)
        self.new_aggregator_tracker(pricing_method=pricing_method,
                                    aggregate_preferred_demand_profile=aggregate_preferred_demand_profile)

        if write_to_file_path is not None:
            self.write_to_file(folder=write_to_file_path, date_time=date_time)
        else:
            self.write_to_file("data/")
        if backup_file_path is not None:
            self.write_to_file(folder=backup_file_path, date_time=date_time)
        print("0. Aggregator is created. ")

        prices, consumption_cost, inconvenience, step, \
        new_aggregate_demand_profile, new_aggregate_battery_profile, time_pricing \
            = self.pricing(num_iteration=0,
                           aggregate_demand_profile=aggregate_preferred_demand_profile,
                           aggregate_battery_profile=[0] * len(aggregate_preferred_demand_profile),
                           aggregate_inconvenience=0)
        return prices, consumption_cost

    def write_to_file(self, folder, date_time=None):
        if not folder.endswith("/"):
            folder += "/"
        folder += "data/"
        path = Path(folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)
        if date_time is None:
            file_name = f"{folder}{file_pricing_table_pkl}"
        else:
            file_name = f"{folder}{date_time}_{file_pricing_table_pkl}"
        with open(file_name, 'wb+') as f:
            pickle.dump(self.pricing_table, f, pickle.HIGHEST_PROTOCOL)
        f.close()

    def new_aggregator_tracker(self, pricing_method, aggregate_preferred_demand_profile):
        self.tracker = Tracker()
        self.tracker.new(name=f"{pricing_method}_agg")
        self.tracker.update(num_record=0, demands=aggregate_preferred_demand_profile)

        self.final = Tracker()
        self.final.new(name=f"{pricing_method}_agg_final")
        self.final.update(num_record=0, demands=aggregate_preferred_demand_profile)

    def pricing(self, num_iteration, aggregate_demand_profile, aggregate_battery_profile,
                total_obj=None,
                aggregate_inconvenience=0, finalising=False,
                min_step_size=min_step, roundup_tiny_step=False, print_steps=False):

        aggregate_demand_profile = self.__convert_demand_profile(aggregate_demand_profile)
        aggregate_battery_profile = self.__convert_demand_profile(aggregate_battery_profile)

        step = 1
        inconvenience = aggregate_inconvenience
        new_aggregate_demand_profile = aggregate_demand_profile[:]
        new_aggregate_battery_profile = aggregate_battery_profile[:]
        time_pricing = 0

        if num_iteration == 0 or finalising:
            prices, consumption_cost \
                = aggregator_pricing.prices_and_cost(pricing_table=self.pricing_table,
                                                     aggregate_demand_profile=aggregate_demand_profile,
                                                     cost_function=self.cost_function_type)
            if num_iteration == 0:
                self.init_cost = consumption_cost
                self.init_demand_max = max(new_aggregate_demand_profile)
                print(f"{num_iteration}. "
                      f"Best step size {round(1, 6)}, "
                      f"{0} iterations, "
                      f"obj {consumption_cost}, "
                      f"change of obj {consumption_cost}, "
                      f"using {self.pricing_method}")

            self.final.update(num_record=num_iteration, penalty=inconvenience,
                              demands=new_aggregate_demand_profile,
                              battery_profile=new_aggregate_battery_profile,
                              init_demand_max=self.init_demand_max,
                              prices=prices, cost=consumption_cost, init_cost=self.init_cost)
        else:
            aggregate_demand_profile_fw_pre = self.tracker.data[s_demand][num_iteration - 1][:]
            aggregate_battery_profile_fw_pre = self.tracker.data[b_profile][num_iteration - 1][:]
            inconvenience_fw_pre = self.tracker.data[s_penalty][num_iteration - 1]
            price_fw_pre = self.tracker.data[p_prices][num_iteration - 1][:]
            total_cost_fw_pre = self.tracker.data[p_cost][num_iteration - 1]
            new_aggregate_demand_profile, new_aggregate_battery_profile, \
            step, prices, consumption_cost, inconvenience, time_pricing \
                = aggregator_pricing.find_step_size(num_iteration=num_iteration,
                                                    pricing_method=self.pricing_method,
                                                    pricing_table=self.pricing_table,
                                                    aggregate_demand_profile_new=aggregate_demand_profile,
                                                    aggregate_demand_profile_fw_pre=aggregate_demand_profile_fw_pre,
                                                    aggregate_battery_profile_new=aggregate_battery_profile,
                                                    aggregate_battery_profile_fw_pre=aggregate_battery_profile_fw_pre,
                                                    total_inconvenience_new=aggregate_inconvenience,
                                                    total_inconvenience_fw_pre=inconvenience_fw_pre,
                                                    total_obj_new=total_obj,
                                                    price_fw_pre=price_fw_pre,
                                                    total_cost_fw_pre=total_cost_fw_pre,
                                                    min_step_size=min_step_size,
                                                    roundup_tiny_step=roundup_tiny_step,
                                                    print_steps=print_steps)

            obj_fw = consumption_cost + inconvenience

            if total_obj is not None and obj_fw > total_obj:
                print("obj fw > total_obj")

        if not finalising:
            self.tracker.update(num_record=num_iteration, penalty=inconvenience,
                                demands=new_aggregate_demand_profile,
                                battery_profile=new_aggregate_battery_profile,
                                init_demand_max=self.init_demand_max,
                                prices=prices, cost=consumption_cost, init_cost=self.init_cost,
                                run_time=time_pricing, step=step)

        return prices, consumption_cost, inconvenience, step, \
               new_aggregate_demand_profile, new_aggregate_battery_profile, time_pricing

    def compute_start_time_probabilities(self):
        history_steps = list(self.tracker.data[p_step].values())
        self.start_time_probability \
            = aggregator_pricing.compute_start_time_probabilities(history_steps=history_steps)
        return self.start_time_probability

    def __convert_demand_profile(self, aggregate_demand_profile_interval):
        num_intervals = len(aggregate_demand_profile_interval)
        num_intervals_periods = int(num_intervals / self.num_periods)
        aggregate_demand_profile_period = aggregate_demand_profile_interval
        if num_intervals != self.num_periods:
            aggregate_demand_profile_period = [sum(x) for x in
                                               grouper(num_intervals_periods, aggregate_demand_profile_interval)]
        return aggregate_demand_profile_period

    def __existing_pricing_table(self, file_folder, date_time=None):
        if date_time is None:
            file_name = f"{file_folder}{file_pricing_table_pkl}"
        else:
            file_name = f"{file_folder}data/{date_time}_{file_pricing_table_pkl}"
        with open(file_name, 'rb') as f:
            pricing_table = pickle.load(f)
        f.close()

        return pricing_table
