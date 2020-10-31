from pandas import read_csv
import pickle
from math import ceil
from pathlib import Path
from fw_ddsm.cfunctions import *
from fw_ddsm.parameter import *
from more_itertools import grouper


class Aggregator:

    def __init__(self, num_periods=no_periods, cost_function="piece-wise"):

        self.num_periods = num_periods
        self.pricing_table = dict()
        self.data = dict()
        self.cost_function_type = cost_function


    def read(self, read_from_file):
        read_from_file = read_from_file if read_from_file.endswith("/") \
            else read_from_file + "/"
        self.data = self.__existing_aggregator(f"{read_from_file}{file_aggregator_pkl}")
        self.pricing_table = self.__existing_pricing_table(f"{read_from_file}{file_pricing_table_pkl}")
        print("Aggregator is read. ")

    def new(self, normalised_pricing_table_csv, aggregate_preferred_demand_profile, algorithms_options,
            weight=pricing_table_weight, write_to_file_path=None):

        num_intervals = len(aggregate_preferred_demand_profile)
        num_intervals_periods = int(num_intervals / self.num_periods)
        if num_intervals != self.num_periods:
            aggregate_preferred_demand_profile = [sum(x) for x in
                                                  grouper(num_intervals_periods, aggregate_preferred_demand_profile)]

        maximum_demand_level = max(aggregate_preferred_demand_profile)
        self.pricing_table = self.__new_pricing_table(normalised_pricing_table_csv, maximum_demand_level,
                                                      weight, write_to_file_path)
        print("Pricing table is created. ")
        self.data = self.__new_aggregator(aggregate_preferred_demand_profile, algorithms_options, write_to_file_path)
        print("Aggregator is created. ")


    def update(self, num_iteration, pricing_method, step=None, prices=None, consumption_cost=None,
               demands=None, inconvenience_cost=None, runtime=None):
        if step is not None:
            self.data[pricing_method][k0_step][num_iteration] = step
        if prices is not None:
            self.data[pricing_method][k0_prices][num_iteration] = prices
        if consumption_cost is not None:
            self.data[pricing_method][k0_cost][num_iteration] = consumption_cost
        if demands is not None:
            self.data[pricing_method][k0_demand][num_iteration] = demands
            self.data[pricing_method][k0_demand_max][num_iteration] = max(demands)
            self.data[pricing_method][k0_demand_total][num_iteration] = sum(demands)
            self.data[pricing_method][k0_par][num_iteration] = max(demands) / average(demands)
        if inconvenience_cost is not None:
            self.data[pricing_method][k0_penalty][num_iteration] = inconvenience_cost
        if runtime is not None:
            self.data[pricing_method][k0_time][num_iteration] = runtime


    def prices_and_cost(self, aggregate_demand_profile):
        prices = []
        consumption_cost = 0

        price_levels = self.pricing_table[k0_price_levels]
        for demand_period, demand_level_period in \
                zip(aggregate_demand_profile, self.pricing_table[k0_demand_table].values()):
            demand_level = list(demand_level_period.values())
            level = bisect_left(demand_level, demand_period)
            if level != len(demand_level):
                price = price_levels[level]
            else:
                price = price_levels[-1]
            prices.append(price)

            if "piece-wise" in self.cost_function_type and level > 0:
                consumption_cost += demand_level[0] * price_levels[0]
                consumption_cost += (demand_period - demand_level[level - 1]) * price
                consumption_cost += sum([(demand_level[i] - demand_level[i - 1]) *
                                         price_levels[i] for i in range(1, level)])
            else:
                consumption_cost += demand_period * price

        consumption_cost = round(consumption_cost, 2)

        return prices, consumption_cost


    def find_step_size(self, num_iteration, demand_profile, inconvenience, pricing_method):

        num_intervals = len(demand_profile)
        if num_intervals != self.num_periods:
            num_intervals_period = int(num_intervals / self.num_periods)
            demand_profile = [sum(x) for x in grouper(num_intervals_period, demand_profile)]


        print("Start finding the step size. ")

        demand_profile_fw_pre = self.data[pricing_method][k0_demand][num_iteration - 1][:]
        inconvenience_fw_pre = self.data[pricing_method][k0_penalty][num_iteration - 1]
        price_fw = self.data[pricing_method][k0_prices][num_iteration - 1][:]
        cost_fw = self.data[pricing_method][k0_cost][num_iteration - 1]

        demand_profile_fw = demand_profile_fw_pre[:]
        inconvenience_fw = inconvenience_fw_pre
        change_of_inconvenience = inconvenience - inconvenience_fw_pre
        # print("change of inconvenience", change_of_inconvenience)

        demand_profile_changed = [d_n - d_p for d_n, d_p in zip(demand_profile, demand_profile_fw_pre)]
        step_size_final = 0
        min_step_size = 0.001
        gradient = -999
        num_itrs = 0
        while gradient < 0 and step_size_final < 1:
            step_profile = []
            for dp, dn, demand_levels_period in \
                    zip(demand_profile_fw_pre, demand_profile, self.pricing_table[k0_demand_table].values()):
                d_levels = list(demand_levels_period.values())
                min_demand_level = min(d_levels)
                max_demand_level = d_levels[-1]
                second_max_demand_level = d_levels[-2]
                if dn < dp < min_demand_level or dp < dn < min_demand_level or dn > dp > second_max_demand_level \
                        or dp > dn > max_demand_level or dn == dp:
                    step = 1
                else:
                    dd = dn - dp
                    dl = find_ge(d_levels, dp) + 0.01 if dd > 0 else find_le(d_levels, dp) - 0.01
                    step = (dl - dp) / dd
                    step = ceil(step * 1000) / 1000
                    step = step if step > min_step_size else 1
                    # step = max(step, min_step_size)

                step_profile.append(step)

            # print("step profile", step_profile)
            step_size_incr = min(step_profile)
            # print(counter, temp_step_size)
            demand_profile_fw_temp = [d_p + (d_n - d_p) * step_size_incr for d_p, d_n in
                                      zip(demand_profile_fw_pre, demand_profile)]
            price_fw_temp, cost_fw_temp = self.prices_and_cost(demand_profile_fw_temp)
            # print("cost fw temp", cost_fw_temp)

            gradient = sum([d_c * p_fw for d_c, p_fw in
                            zip(demand_profile_changed, price_fw_temp)]) + change_of_inconvenience
            # print("gradient", gradient)

            demand_profile_fw_pre = demand_profile_fw_temp[:]
            step_size_final_temp = step_size_final + step_size_incr
            # print(step_size_final_temp)

            if gradient < 0 and step_size_final_temp < 1:
                step_size_final = step_size_final_temp
                demand_profile_fw = demand_profile_fw_temp[:]
                # print("best step size", best_step_size)
                price_fw = price_fw_temp[:]
                # print("after fw", price_day)
                cost_fw = cost_fw_temp
                inconvenience_fw = inconvenience_fw_pre + step_size_final * change_of_inconvenience
                # print("cost", cost)
                num_itrs += 1

        print(f"best step size {step_size_final} found in {num_itrs} iterations at the cost of {cost_fw}")

        return demand_profile_fw, step_size_final, price_fw, cost_fw, inconvenience_fw


    def  __new_pricing_table(self, normalised_pricing_table_csv, maximum_demand_level, weight=pricing_table_weight,
                             write_to_file_path=None):
    # ---------------------------------------------------------------------- #
    # normalised_pricing_table_csv:
    #       the path of the CSV file of the normalised pricing table
    # demand_level_scalar:
    #       the scalar for rescaling the normalised demand levels
    # ---------------------------------------------------------------------- #

        num_periods = self.num_periods
        csv_table = read_csv(normalised_pricing_table_csv, header=None)
        num_levels = len(csv_table.index)
        demand_level_scalar = maximum_demand_level * weight
        csv_table.loc[num_levels + 1] = [csv_table[0].values[-1] * 10] + [demand_level_scalar * 1.2 for _ in range(num_periods)]

        zero_digit = 100
        pricing_table = dict()
        pricing_table[k0_price_levels] = list(csv_table[0].values)
        pricing_table[k0_demand_table] = dict()
        pricing_table[k0_demand_table] = \
            {period:
                 {level:
                     ceil(csv_table[period + 1].values[level] * demand_level_scalar / zero_digit) * zero_digit
                  for level in range(len(csv_table[period + 1]))}
             for period in range(num_periods)}

        if write_to_file_path is not None:
            write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
                else write_to_file_path + "/"
            path = Path(write_to_file_path)
            if not path.exists():
                path.mkdir(mode=0o777, parents=True, exist_ok=False)

            with open(f"{write_to_file_path}{file_pricing_table_pkl}", 'wb+') as f:
                pickle.dump(pricing_table, f, pickle.HIGHEST_PROTOCOL)
            f.close()

        return pricing_table


    def __existing_pricing_table(self, file_path):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #

        with open(file_path, 'rb') as f:
            pricing_table = pickle.load(f)
        f.close()

        return pricing_table


    def __new_aggregator(self, aggregate_preferred_demand_profile, algorithms_options, write_to_file_path=None):
        max_demand = max(aggregate_preferred_demand_profile)
        total_demand = sum(aggregate_preferred_demand_profile)
        par = round(max_demand / average(aggregate_preferred_demand_profile), 2)

        aggregator = dict()
        aggregator[k0_demand_max] = max_demand
        aggregator[k0_demand] = aggregate_preferred_demand_profile
        for algorithm in algorithms_options.values():
            for alg in algorithm.values():
                if "fw" in alg:
                    aggregator[alg] = dict()
                    aggregator[alg][k0_demand] = dict()
                    aggregator[alg][k0_demand_max] = dict()
                    aggregator[alg][k0_demand_total] = dict()
                    aggregator[alg][k0_par] = dict()
                    aggregator[alg][k0_penalty] = dict()
                    aggregator[alg][k0_final] = dict()
                    aggregator[alg][k0_prices] = dict()
                    aggregator[alg][k0_cost] = dict()
                    aggregator[alg][k0_step] = dict()
                    aggregator[alg][k0_time] = dict()

                    aggregator[alg][k0_demand][0] = aggregate_preferred_demand_profile
                    aggregator[alg][k0_demand_max][0] = max_demand
                    aggregator[alg][k0_demand_total][0] = total_demand
                    aggregator[alg][k0_par][0] = par
                    aggregator[alg][k0_par][0] = par
                    aggregator[alg][k0_penalty][0] =0
                    aggregator[alg][k0_cost][0] = None
                    aggregator[alg][k0_step][0] = 1
                    aggregator[alg][k0_time][0] = 0

        if write_to_file_path is not None:
            write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
                else write_to_file_path + "/"
            path = Path(write_to_file_path)
            if not path.exists():
                path.mkdir(mode=0o777, parents=True, exist_ok=False)

            with open(f"{write_to_file_path}{file_aggregator_pkl}", 'wb+') as f:
                pickle.dump(aggregator, f, pickle.HIGHEST_PROTOCOL)
            f.close()

        return aggregator


    def __existing_aggregator(self, file_path):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #

        with open(file_path, 'rb') as f:
            aggregator = pickle.load(f)
        f.close()

        return aggregator



