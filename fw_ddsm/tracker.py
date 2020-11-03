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
                tracker[method][k0_time][num_record] = run_time
        return tracker

    def write_to_csv(self, write_to_folder, tracker_name,
                     print_demands=True, print_prices=True, print_summary=True,
                     write_to_parent_folder=None):

        write_to_folder = write_to_folder if write_to_folder.endswith("/") \
            else write_to_folder + "/"
        if write_to_parent_folder is not None:
            write_to_parent_folder = write_to_parent_folder if write_to_parent_folder.endswith("/") \
                else write_to_parent_folder + "/"
        path = Path(write_to_folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        def reduction_percentage(x):
            return round((x.iloc[0] - x.iloc[-1]) / x.iloc[0], 2)

        def append_to_overview(f_overview, df_data):
            if Path(file_overview).exists():
                df_data.to_csv(rf"{f_overview}")
            else:
                df_data.to_csv(rf"{f_overview}", mode='a', header=False)

        for method in self.data:
            data_method = self.data[method].copy()
            data_demands = data_method.pop(k0_demand)
            data_prices = data_method.pop(k0_prices)
            if print_demands:
                df.from_dict(data_demands).to_csv(rf"{write_to_folder}{method}_{tracker_name}_demands.csv")
            if print_prices:
                df.from_dict(data_prices).to_csv(rf"{write_to_folder}{method}_{tracker_name}_prices.csv")
            if print_summary:
                summary = {k: data_method[k] for k in [k0_demand_max, k0_par, k0_cost, k0_time]}
                df_summary = df.from_dict(summary)
                df_summary.to_csv(rf"{write_to_folder}{method}_{tracker_name}_summary.csv")
                df_summary_agg = df_summary[[k0_demand_max, k0_par, k0_cost]].aggregate(reduction_percentage)
                df_summary_agg.loc[k0_time] = df_summary[k0_time].mean()
                df_summary_agg = df_summary_agg.to_frame().transpose()

                file_overview = f"{write_to_folder}{tracker_name}_overview.csv"
                if Path(file_overview).exists():
                    df_summary_agg.to_csv(rf"{file_overview}")
                else:
                    df_summary_agg.to_csv(rf"{file_overview}", mode='a', header=False)

                file_overview = f"{write_to_folder}{tracker_name}_overview.csv"
                file_parent_overview = f"{write_to_parent_folder}{tracker_name}_overview.csv"
                append_to_overview(file_overview, df_summary_agg)
                append_to_overview(file_parent_overview, df_summary_agg)

    def draw_graphs(self):
        return 0
