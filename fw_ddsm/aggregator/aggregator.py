from pandas import read_csv
import pickle
from pathlib import Path
from fw_ddsm.cfunctions import *
from fw_ddsm.parameter import *

class Aggregator:

    def __init__(self, num_periods=no_periods, cost_function="piece-wise"):

        self.num_periods = num_periods
        self.pricing_table = dict()
        self.data = dict()
        self.cost_function_type = cost_function


    def read(self, read_from_file):
        self.data = self.__existing_aggregator(read_from_file)
        print("Aggregator is read. ")

    def new(self, normalised_pricing_table_csv, aggregate_preferred_demand_profile, algorithms_options,
            weight=pricing_table_weight, write_to_file_path=None):

        maximum_demand_level = max(aggregate_preferred_demand_profile)
        self.pricing_table = self.__new_pricing_table(normalised_pricing_table_csv, maximum_demand_level,
                                                      weight)
        print("Pricing table is created. ")
        self.data = self.__new_aggregator(aggregate_preferred_demand_profile, algorithms_options, write_to_file_path)
        print("Aggregator is created. ")


    def update(self, num_iteration, pricing_method, step=None, prices=None, consumption_cost=None,
               demands=None, inconvenience_cost=None):
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


    def pricing(self, aggregate_demand_profile):
        prices = []
        consumption_cost = 0
        pricing_table = self.pricing_table
        cost_function = self.cost_function_type

        price_levels = pricing_table[k0_price_levels]
        for demand_period, demand_level_period in \
                zip(aggregate_demand_profile, pricing_table[k0_demand_table].values()):
            demand_level = list(demand_level_period.values())
            level = bisect_left(demand_level, demand_period)
            if level != len(demand_level):
                price = price_levels[level]
            else:
                price = price_levels[-1]
            prices.append(price)

            if "piece-wise" in cost_function and level > 0:
                consumption_cost += demand_level[0] * price_levels[0]
                consumption_cost += (demand_period - demand_level[level - 1]) * price
                consumption_cost += sum([(demand_level[i] - demand_level[i - 1]) *
                                         price_levels[i] for i in range(1, level)])
            else:
                consumption_cost += demand_period * price

        consumption_cost = round(consumption_cost, 2)

        return prices, consumption_cost


    def  __new_pricing_table(self, normalised_pricing_table_csv, maximum_demand_level, weight=pricing_table_weight):
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

        zero_digit = 2
        pricing_table = dict()
        pricing_table[k0_price_levels] = list(csv_table[0].values)
        pricing_table[k0_demand_table] = dict()
        pricing_table[k0_demand_table] = \
            {period:
                 {level:
                      round(csv_table[period + 1].values[level] * demand_level_scalar, -zero_digit)
                  for level in range(len(csv_table[period + 1]))}
             for period in range(num_periods)}

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

                aggregator[alg][k0_demand][0] = aggregate_preferred_demand_profile
                aggregator[alg][k0_demand_max][0] = max_demand
                aggregator[alg][k0_demand_total][0] = total_demand
                aggregator[alg][k0_par][0] = par
                aggregator[alg][k0_par][0] = par
                aggregator[alg][k0_penalty][0] =None
                aggregator[alg][k0_cost][0] = None
                aggregator[alg][k0_step][0] = 1

        if write_to_file_path is not None:
            write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
                else write_to_file_path + "/"
            path = Path(write_to_file_path)
            if not path.exists():
                path.mkdir(mode=0o777, parents=True, exist_ok=False)

            with open(f"{write_to_file_path}aggregator.pkl", 'wb+') as f:
                pickle.dump(aggregator, f, pickle.HIGHEST_PROTOCOL)
            f.close()

        return aggregator


    def __existing_aggregator(self, file_path):
        # ---------------------------------------------------------------------- #
        # ---------------------------------------------------------------------- #

        file_path = file_path if file_path.endswith("/") else file_path + "/"

        with open(file_path + "aggregator" + '.pkl', 'rb') as f:
            aggregator = pickle.load(f)
        f.close()

        return aggregator



