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
        __new_record_type(k0_demand_reduction)
        __new_record_type(k0_par)
        __new_record_type(k0_penalty)
        __new_record_type(k0_prices)
        __new_record_type(k0_cost)
        __new_record_type(k0_cost_reduction)
        __new_record_type(k0_step)
        __new_record_type(k0_time)

    def read(self, method_tracker, method):
        self.data[method] = dict()
        self.data[method] = method_tracker

    def update(self, num_record, method, tracker=None, demands=None, prices=None, penalty=None,
               run_time=None, cost=None, step=None, init_demand_max=None, init_cost=None):
        if tracker is None:
            tracker = self.data
        if step is not None:
            tracker[method][k0_step][num_record] = round(step, 4)
        if prices is not None:
            tracker[method][k0_prices][num_record] = prices
        if cost is not None:
            tracker[method][k0_cost][num_record] = round(cost, 2)
            if init_cost is not None:
                tracker[method][k0_cost_reduction][num_record] = round((init_cost - cost) / init_cost, 2)
        if penalty is not None:
            tracker[method][k0_penalty][num_record] = round(penalty, 2)
        if demands is not None:
            demand_max = round(max(demands), 2)
            tracker[method][k0_demand][num_record] = demands
            tracker[method][k0_demand_max][num_record] = demand_max
            tracker[method][k0_demand_total][num_record] = round(sum(demands), 2)
            tracker[method][k0_par][num_record] = round(demand_max / average(demands), 2)
            if init_demand_max is not None:
                tracker[method][k0_demand_reduction][num_record] \
                    = round((init_demand_max - demand_max) / init_demand_max, 2)
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
            others[method] = {k: data_method[k]
                              for k in [k0_par, k0_demand_reduction, k0_cost_reduction, k0_penalty,
                                        k0_demand_total, k0_demand_max, k0_cost]}

        return demands, prices, others, times
