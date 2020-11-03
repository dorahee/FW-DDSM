import pandas_bokeh
from pathlib import Path
from pandas import DataFrame as df
from datetime import date, datetime
from fw_ddsm.parameter import *
from fw_ddsm.tracker import *
from pandas_bokeh import *


class Show:

    def __init__(self):

        self.output_folder = ""
        self.aggregator_tracker = Tracker()
        self.aggregator_final = Tracker()
        self.community_tracker = Tracker()
        self.community_final = Tracker()
        self.output_root_folder = ""
        self.output_parent_folder = ""
        self.output_folder = ""

    def set_output_folder(self, output_root_folder):
        if not output_root_folder.endswith("/"):
            output_root_folder += "/"

        this_date = str(date.today())
        this_time = str(datetime.now().time().strftime("%H-%M-%S"))
        self.output_parent_folder = f"{output_root_folder}{this_date}/"
        self.output_folder = f"{self.output_parent_folder}/{this_time}/"
        path = Path(self.output_folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)
        return self.output_parent_folder, self.output_folder

    def set_data(self, algorithms, aggregator_tracker, community_tracker, aggregator_final, community_final=None,
                 print_demands=False, print_prices=False, print_summary=True):
        self.aggregator_tracker = aggregator_tracker
        self.aggregator_final = aggregator_final
        self.community_tracker = community_tracker
        self.community_final = community_final

        agg_demands, agg_prices, agg_others, agg_times = aggregator_tracker.extract_data()
        agg_demands_final, agg_prices_final, agg_others_final, agg_times_final = aggregator_final.extract_data()
        community_demands, community_prices, community_others, community_times = community_tracker.extract_data()

        # df_aggregator = dict()
        # df_final = dict()
        plots_demands = []
        for alg in algorithms.values():
            pricing_method = alg[k2_after_fw]

            df_demands = df.from_dict(agg_demands[pricing_method])
            df_prices = df.from_dict(agg_prices[pricing_method])
            df_others = df.from_dict(agg_others[pricing_method])

            p_demands = df_demands.iloc[:, [0, df_demands.columns[-1]]] \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kW)", title=pricing_method,
                            plot_data_points=True,
                            # show_figure=False
                            )
            plots_demands.append(p_demands)

            df_demands_final = df.from_dict(agg_demands_final[pricing_method])
            df_prices_final = df.from_dict(agg_prices_final[pricing_method])
            df_others_final = df.from_dict(agg_others_final[pricing_method])

            # df_aggregator[pricing_method] = dict()
            # df_aggregator[pricing_method][k0_demand] = df_demands
            # df_aggregator[pricing_method][k0_prices] = df_prices
            # df_aggregator[pricing_method][k0_others] = df_others
            #
            # df_final[pricing_method] = dict()
            # df_final[pricing_method][k0_demand] = df_demands_final
            # df_final[pricing_method][k0_prices] = df_prices_final
            # df_final[pricing_method][k0_others] = df_others_final

            if print_demands:
                df_demands.to_csv(r"{}{}_aggregator_demands.csv".format(self.output_folder, pricing_method))
                df_demands_final.to_csv(r"{}{}_aggregator_demands_final.csv".format(self.output_folder, pricing_method))
            if print_prices:
                df_prices.to_csv(r"{}{}_aggregator_prices.csv".format(self.output_folder, pricing_method))
                df_prices_final.to_csv(r"{}{}_aggregator_prices_final.csv".format(self.output_folder, pricing_method))
            if print_summary:
                df_others.to_csv(r"{}{}_aggregator_others.csv".format(self.output_folder, pricing_method))
                df_others_final.to_csv(r"{}{}_aggregator_others_final.csv".format(self.output_folder, pricing_method))

        agg_times.update(community_times)
        df_times = df.from_dict(agg_times)
        df_times.to_csv(r"{}run_times.csv".format(self.output_folder))

        # files to print
        print()
        # community_times

    def draw_graphs(self):
        return 0
