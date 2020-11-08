from pandas import DataFrame as df
from scripts.custom_functions import *
from fw_ddsm.parameter import *


class Tracker:

    def __init__(self, ):
        self.data = dict()
        self.name = "name"

    def new(self, name=""):
        self.name = name

        for key in [k_demand, k_demand_max, k_demand_total, k_demand_reduction, k_par,
                    k0_penalty, k0_prices, k0_cost, k0_cost_reduction, k0_step, k0_time]:
            self.data[key] = dict()

    def read(self, existing_tracker):
        self.data = dict()
        self.data = existing_tracker.copy()

    def update(self, num_record, tracker_data=None, demands=None, prices=None, penalty=None,
               run_time=None, cost=None, step=None, init_demand_max=None, init_cost=None):
        if tracker_data is None:
            tracker_data = self.data
        if step is not None:
            tracker_data[k0_step][num_record] = round(step, 4)
        if prices is not None:
            tracker_data[k0_prices][num_record] = prices
        if cost is not None:
            tracker_data[k0_cost][num_record] = round(cost, 2)
            if init_cost is not None:
                tracker_data[k0_cost_reduction][num_record] = round((init_cost - cost) / init_cost, 2)
        if penalty is not None:
            tracker_data[k0_penalty][num_record] = round(penalty, 2)
        if demands is not None:
            demand_max = round(max(demands), 2)
            tracker_data[k_demand][num_record] = demands
            tracker_data[k_demand_max][num_record] = demand_max
            tracker_data[k_demand_total][num_record] = round(sum(demands), 2)
            tracker_data[k_par][num_record] = round(demand_max / average(demands), 2)
            if init_demand_max is not None:
                tracker_data[k_demand_reduction][num_record] \
                    = round((init_demand_max - demand_max) / init_demand_max, 2)
        if run_time is not None:
            tracker_data[k0_time][num_record] = round(run_time, 4)

        return tracker_data

    def extract_data(self):

        demands = self.data[k_demand]
        prices = self.data[k0_prices]
        others = {k: self.data[k]
                  for k in [k_par, k_demand_reduction, k0_cost_reduction, k0_penalty,
                            k_demand_total, k_demand_max, k0_cost, k0_time]}
        return demands, prices, others

    def write_to_file(self, folder, print_demands=True, print_prices=True, print_others=True):
        demands, prices, others = self.extract_data()
        if print_demands:
            df_demands = df.from_dict(demands).div(1000)
            df_demands.to_csv(r"{}{}_demands.csv".format(folder, self.name))
        if print_prices:
            df_prices = df.from_dict(prices)
            df_prices.to_csv(r"{}{}_prices.csv".format(folder, self.name))
        if print_others:
            df_others = df.from_dict(others)
            df_others.to_csv(r"{}{}_others.csv".format(folder, self.name))
