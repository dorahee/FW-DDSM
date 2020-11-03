from pathlib import Path
from pandas import DataFrame as df
from fw_ddsm.cfunctions import *
from fw_ddsm.parameter import *


class Tracker:

    def __init__(self):
        self.data = dict()

    def new(self, method):

        def __new_record_type(key):
            self.data[method][key] = dict()

        self.data[method] = dict()
        __new_record_type(k0_demand)
        __new_record_type(k0_demand_max)
        __new_record_type(k0_demand_total)
        __new_record_type(k0_par)
        __new_record_type(k0_penalty)
        __new_record_type(k0_prices)
        __new_record_type(k0_cost)
        __new_record_type(k0_step)
        __new_record_type(k0_time)

    def read(self, method_tracker, method):
        self.data[method] = dict()
        self.data[method] = method_tracker

    def update(self, num_record, method, tracker=None, demands=None, prices=None, penalty=None,
               run_time=None, cost=None, final=None, step=None):

        if tracker is None:
            tracker = self.data
        if final is not None:
            # demands = self.__convert_demand_profile(demands)
            tracker[method][k0_final][k0_demand] = demands
            tracker[method][k0_final][k0_demand_max] = max(demands)
            tracker[method][k0_final][k0_par] = max(demands) / average(demands)
            tracker[method][k0_final][k0_prices] = prices
            tracker[method][k0_final][k0_cost] = cost
        else:
            if step is not None:
                tracker[method][k0_step][num_record] = step
            if prices is not None:
                tracker[method][k0_prices][num_record] = prices
            if cost is not None:
                tracker[method][k0_cost][num_record] = cost
            if penalty is not None:
                tracker[method][k0_penalty][num_record] = penalty
            if demands is not None:
                tracker[method][k0_demand][num_record] = demands
                tracker[method][k0_demand_max][num_record] = max(demands)
                tracker[method][k0_demand_total][num_record] = sum(demands)
                tracker[method][k0_par][num_record] = max(demands) / average(demands)
            if run_time is not None:
                tracker[method][k0_time][num_record] = round(run_time, 4)
        return tracker

    def extract_data(self):

        demands = dict()
        prices = dict()
        others = dict()
        times = dict()
        for method in self.data:
            data_method = self.data[method]
            demands[method] = data_method[k0_demand]
            prices[method] = data_method[k0_prices]
            times[method] = data_method[k0_time]
            others[method] = {k:data_method[k] for k in [k0_demand_max, k0_par, k0_cost]}

        return demands, prices, others, times



