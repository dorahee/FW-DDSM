from fw_ddsm.parameter import *
from fw_ddsm.cfunctions import *


class Tracker:

    def __init__(self, method=""):
        self.data = dict()
        self.method = method

    def new(self, method):
        self.method = method
        self.data[method] = dict()
        self.__new_record_type(k0_demand)
        self.__new_record_type(k0_demand_max)
        self.__new_record_type(k0_demand_total)
        self.__new_record_type(k0_par)
        self.__new_record_type(k0_penalty)
        self.__new_record_type(k0_final)
        self.__new_record_type(k0_prices)
        self.__new_record_type(k0_cost)
        self.__new_record_type(k0_step)
        self.__new_record_type(k0_time)

    def read(self, tracker_dict, method):
        self.data = dict()
        self.data = tracker_dict
        self.method = method

    def update(self, num_record, tracker=None, method=None, demands=None, prices=None, penalty=None,
               run_time=None, cost=None, final=None, step=None):
        if method is None:
            method = self.method
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
                tracker[method][k0_time][num_record] = run_time
        return tracker

    def __new_record_type(self, key):
        self.data[self.method][key] = dict()
