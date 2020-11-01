import pandas_bokeh
from pathlib import Path
from pandas import DataFrame as df
from datetime import date, datetime
from fw_ddsm.parameter import *
from fw_ddsm import aggregator
from fw_ddsm import household
from fw_ddsm import community


class Show:

    def __init__(self, output_folder):

        self.aggregator = dict()
        self.aggregator_demands = dict()
        self.aggregator_prices = dict()
        self.aggregator_final = dict()
        self.df_aggregator = df()
        self.df_aggregator_demands = df()
        self.df_aggregator_prices = df()
        self.df_aggregator_final = df()

        self.community_aggregate = dict()
        self.community_aggregate_demands = dict()
        self.community_aggregate_prices = dict()
        self.community_aggregate_final = dict()
        self.df_community_aggregate = df()
        self.df_community_aggregate_demands = df()
        self.df_community_aggregate_prices = df()
        self.df_community_aggregate_final = df()

        self.household = dict()
        self.df_household = df()

        self.scheduling_method = ""
        self.pricing_method = ""

        this_date = str(date.today())
        this_time = str(datetime.now().time().strftime("%H-%M-%S"))
        output_folder = output_folder if output_folder.endswith("/") else output_folder + "/"
        self.output_folder = f"{output_folder}{this_date}/{this_time}/"
        path = Path(self.output_folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

    def set_data(self, aggregator_data, algorithm, household_data=None, community_aggregate=None):
        self.scheduling_method = algorithm[k2_before_fw]
        self.pricing_method = algorithm[k2_after_fw]

        if aggregator_data is not None:
            self.aggregator = aggregator_data[self.pricing_method]
            self.aggregator_demands = self.aggregator.pop(k0_demand, None)
            self.aggregator_prices = self.aggregator.pop(k0_prices, None)
            self.aggregator_final = self.aggregator.pop(k0_final, None)

            self.df_aggregator = df.from_dict(self.aggregator)
            self.df_aggregator_demands = df.from_dict(self.aggregator_demands)
            self.df_aggregator_prices = df.from_dict(self.aggregator_prices)
            self.df_aggregator_final = df.from_dict(self.aggregator_final)

        if community_aggregate is not None:
            self.community_aggregate = community_aggregate[self.scheduling_method]
            self.community_aggregate_demands = self.community_aggregate.pop(k0_demand)
            self.community_aggregate_prices = self.community_aggregate.pop(k0_prices)
            self.community_aggregate_final = self.community_aggregate.pop(k0_final)

            self.df_community_aggregate = df.from_dict(self.community_aggregate)
            self.df_community_aggregate_demands = df.from_dict(self.community_aggregate_demands)
            self.df_community_aggregate_prices = df.from_dict(self.community_aggregate_prices)
            self.df_community_aggregate_final = df.from_dict(self.community_aggregate_final)

        if household_data is not None:
            self.household = household_data
            self.df_household = df.from_dict(self.household[self.scheduling_method])

    def write_to_csv(self):

        def to_csv(data_df, demand_df, prices_df, final_df, name, method):
            data_df.to_csv(f"{self.output_folder}{method}_{name}_summary.csv")
            demand_df.to_csv(rf"{self.output_folder}{method}_{name}_demands.csv")
            prices_df.to_csv(rf"{self.output_folder}{method}_{name}_prices.csv")
            final_df.to_csv(rf"{self.output_folder}{method}_{name}_final.csv")

        to_csv(data_df=self.df_aggregator, demand_df=self.df_aggregator_demands,
               prices_df=self.df_aggregator_prices, final_df=self.df_aggregator_final,
               method=self.pricing_method, name="aggregator")

        if not self.community_aggregate:
            to_csv(data_df=self.df_community_aggregate, demand_df=self.df_community_aggregate_demands,
                   prices_df=self.df_community_aggregate_prices, final_df=self.df_community_aggregate_final,
                   method=self.scheduling_method, name="community")

        print("The household data and the aggregator data are written to CSV files.")

    def draw_graphs(self):
        return 0


