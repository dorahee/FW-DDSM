from pandas import read_csv
import pickle
from math import ceil
from pathlib import Path
from more_itertools import grouper
from time import time
from fw_ddsm.cfunctions import *
from fw_ddsm.parameter import *
from fw_ddsm.tracker import *


class Aggregator:

    def __init__(self, num_periods=no_periods, cost_function="piece-wise"):
        self.num_periods = num_periods
        self.cost_function_type = cost_function
        self.pricing_table = dict()
        self.aggregator_tracker = Tracker()
        self.aggregator_final = Tracker()
        self.start_time_probability = None
        self.pricing_method = ""

    def read(self, read_from_folder, pricing_method):
        self.pricing_table = dict()
        self.aggregator_tracker = Tracker()
        self.pricing_method = pricing_method

        read_from_folder = read_from_folder if read_from_folder.endswith("/") \
            else read_from_folder + "/"
        self.pricing_table, aggregator_tracker = self.__existing_aggregator(read_from_folder)
        self.aggregator_tracker.read(aggregator_tracker, method=pricing_method)
        self.aggregator_final.new(method=pricing_method)
        print("Aggregator is read. ")

    def new(self, normalised_pricing_table_csv, aggregate_preferred_demand_profile, pricing_method,
            weight=pricing_table_weight, write_to_file_path=None):
        self.pricing_table = dict()
        self.aggregator_tracker = Tracker()
        self.pricing_method = pricing_method

        aggregate_preferred_demand_profile = self.__convert_demand_profile(aggregate_preferred_demand_profile)
        maximum_demand_level = max(aggregate_preferred_demand_profile)
        self.pricing_table = self.__new_pricing_table(normalised_pricing_table_csv, maximum_demand_level, weight)
        self.aggregator_tracker.new(method=pricing_method)
        self.aggregator_tracker.update(num_record=0, demands=aggregate_preferred_demand_profile)
        self.aggregator_final.new(method=pricing_method)

        if write_to_file_path is not None:
            self.write_to_file(write_to_file_path=write_to_file_path)
        print("Aggregator is created. ")

    def write_to_file(self, write_to_file_path):
        write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
            else write_to_file_path + "/"
        path = Path(write_to_file_path)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        with open(f"{write_to_file_path}{file_pricing_table_pkl}", 'wb+') as f:
            pickle.dump(self.pricing_table, f, pickle.HIGHEST_PROTOCOL)
        f.close()

        if write_to_file_path is not None:
            with open(f"{write_to_file_path}{file_aggregator_pkl}", 'wb+') as f:
                pickle.dump(self.aggregator_tracker.data, f, pickle.HIGHEST_PROTOCOL)
            f.close()

    def pricing(self, num_iteration, aggregate_demand_profile, aggregate_inconvenience=0, finalising=False):

        aggregate_demand_profile = self.__convert_demand_profile(aggregate_demand_profile)
        step = 1
        inconvenience = 0
        new_aggregate_demand_profile = aggregate_demand_profile
        time_pricing = 0

        if num_iteration == 0:
            prices, consumption_cost = self.__prices_and_cost(aggregate_demand_profile)
        else:
            new_aggregate_demand_profile, step, prices, consumption_cost, inconvenience, time_pricing \
                = self.__find_step_size(num_iteration=num_iteration, aggregate_demand_profile=aggregate_demand_profile,
                                        aggregate_inconvenience=aggregate_inconvenience)

        if not finalising:
            self.aggregator_tracker.update(num_record=num_iteration, demands=new_aggregate_demand_profile,
                                           step=step, prices=prices, cost=consumption_cost, penalty=inconvenience,
                                           run_time=time_pricing)
        else:
            self.aggregator_final.update(num_record=num_iteration, demands=new_aggregate_demand_profile,
                                         prices=prices, cost=consumption_cost, penalty=inconvenience)

        return prices, consumption_cost, inconvenience, step, new_aggregate_demand_profile, time_pricing

    def __prices_and_cost(self, aggregate_demand_profile):
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

    def __find_step_size(self, num_iteration, aggregate_demand_profile, aggregate_inconvenience):
        pricing_method = self.pricing_method
        time_begin = time()
        demand_profile_fw_pre = self.aggregator_tracker.data[pricing_method][k0_demand][num_iteration - 1][:]
        inconvenience_fw_pre = self.aggregator_tracker.data[pricing_method][k0_penalty][num_iteration - 1]
        price_fw = self.aggregator_tracker.data[pricing_method][k0_prices][num_iteration - 1][:]
        cost_fw = self.aggregator_tracker.data[pricing_method][k0_cost][num_iteration - 1]

        demand_profile_fw = demand_profile_fw_pre[:]
        inconvenience_fw = inconvenience_fw_pre
        change_of_inconvenience = aggregate_inconvenience - inconvenience_fw_pre
        demand_profile_changed = [d_n - d_p for d_n, d_p in zip(aggregate_demand_profile, demand_profile_fw_pre)]
        step_size_final = 0
        min_step_size = 0.001
        gradient = -999
        num_itrs = 0
        while gradient < 0 and step_size_final < 1:
            step_profile = []
            for dp, dn, demand_levels_period in \
                    zip(demand_profile_fw_pre, aggregate_demand_profile, self.pricing_table[k0_demand_table].values()):
                d_levels = list(demand_levels_period.values())[:-1]
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
            step_size_incr = min(step_profile)

            demand_profile_fw_temp = [d_p + (d_n - d_p) * step_size_incr for d_p, d_n in
                                      zip(demand_profile_fw_pre, aggregate_demand_profile)]
            price_fw_temp, cost_fw_temp = self.__prices_and_cost(demand_profile_fw_temp)
            gradient = sum([d_c * p_fw for d_c, p_fw in
                            zip(demand_profile_changed, price_fw_temp)]) + change_of_inconvenience

            demand_profile_fw_pre = demand_profile_fw_temp[:]
            step_size_final_temp = step_size_final + step_size_incr
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

        print(f"   Found the best step size {step_size_final} in {num_itrs} iterations at the cost of {cost_fw}")
        time_fw = time() - time_begin
        return demand_profile_fw, step_size_final, price_fw, cost_fw, inconvenience_fw, time_fw

    def compute_start_time_probabilities(self, pricing_method):
        prob_dist = []
        history_steps = list(self.aggregator_tracker.data[pricing_method][k0_step].values())
        del history_steps[0]

        for alpha in history_steps:
            if not prob_dist:
                prob_dist.append(1 - alpha)
                prob_dist.append(alpha)
            else:
                prob_dist = [p_d * (1 - alpha) for p_d in prob_dist]
                prob_dist.append(alpha)
        self.start_time_probability = prob_dist.copy()

        return prob_dist

    def __convert_demand_profile(self, aggregate_demand_profile_interval):
        num_intervals = len(aggregate_demand_profile_interval)
        num_intervals_periods = int(num_intervals / self.num_periods)
        aggregate_demand_profile_period = aggregate_demand_profile_interval
        if num_intervals != self.num_periods:
            aggregate_demand_profile_period = [sum(x) for x in
                                               grouper(num_intervals_periods, aggregate_demand_profile_interval)]

        return aggregate_demand_profile_period

    def __new_pricing_table(self, normalised_pricing_table_csv, maximum_demand_level, weight=pricing_table_weight):
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
        csv_table.loc[num_levels + 1] = [csv_table[0].values[-1] * 10] + [demand_level_scalar * 1.2 for _ in
                                                                          range(num_periods)]

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

        return pricing_table

    def __existing_aggregator(self, file_folder):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #
        with open(f"{file_folder}{file_pricing_table_pkl}", 'rb') as f:
            pricing_table = pickle.load(f)
        f.close()

        with open(f"{file_folder}{file_aggregator_pkl}", 'rb') as f:
            aggregator_tracker = pickle.load(f)
        f.close()
        return pricing_table, aggregator_tracker
