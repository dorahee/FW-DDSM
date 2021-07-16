from pathlib import Path
from pandas import DataFrame as df
from datetime import date, datetime
from pandas_bokeh import *
from bokeh.layouts import layout
from bokeh.models import *
from fw_ddsm.tracker import *
import os
import shutil


class Output:

    def __init__(self, output_root_folder="results/", output_parent_folder=None, date_time=None):

        self.output_folder = ""
        # self.aggregator_tracker = Tracker()
        # self.aggregator_final = Tracker()
        # self.community_tracker = Tracker()
        # self.community_final = Tracker()
        self.subfolder_name = ""

        if not output_root_folder.endswith("/"):
            output_root_folder += "/"
        self.output_root_folder = output_root_folder

        if date_time is None:
            this_date = str(date.today())
            this_time = str(datetime.now().time().strftime("%H-%M-%S"))
            self.this_date_time = f"{this_date}_{this_time}"
        else:
            self.this_date_time = date_time
        if output_parent_folder is not None:
            self.output_parent_folder = f"{self.output_root_folder}{output_parent_folder}/"
        else:
            self.output_parent_folder = f"{self.output_root_folder}{self.this_date_time}/"

    def new_output_folder(self,
                          num_households=no_households,
                          inconvenience_cost_weight=care_f_weight,
                          num_dependent_tasks=no_tasks_dependent,
                          num_full_flex_task_min=no_full_flex_tasks_min,
                          num_semi_flex_task_min=no_semi_flex_tasks_min,
                          battery_size=False, efficiency=0,
                          repeat=None, folder_id=None):

        self.subfolder_name = f"h{num_households}-w{inconvenience_cost_weight}-dt{num_dependent_tasks}" \
            f"-fft{num_full_flex_task_min}-sft{num_semi_flex_task_min}-b{int(battery_size)}" \
            f"-e{efficiency}"
        if repeat is not None:
            self.subfolder_name += f"-r{repeat}"
        if folder_id is not None:
            self.subfolder_name += f"-id{folder_id}"
        self.output_folder \
            = f"{self.output_parent_folder}{self.subfolder_name}" + "/"
        path = Path(self.output_folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        return self.output_folder, self.output_parent_folder, self.this_date_time

    def save_to_output_folder(self, algorithm,
                              aggregator_tracker, community_tracker,
                              aggregator_final, community_final=None,
                              params_tracker=None,
                              obj_par=False, obj_cost=True, obj_inconvenience=True,
                              print_demands=True, print_batteries=True, print_prices=True,
                              print_summary=True, print_debugger=True):

        pricing_method = algorithm[m_after_fw]
        scheduling_method = algorithm[m_before_fw]
        x_ticks = [0] + [i for i in range(1, 48) if (i + 1) % 12 == 0]

        # ------------------------------ results from community ------------------------------

        community_demands, community_batteries, community_prices, community_others, community_debugger \
            = community_tracker.extract_data()
        community_times = community_others[t_time]
        df_com_debugger = df.from_dict(community_debugger)
        df_com_demands = df.from_dict(community_demands).div(1000)
        df_com_batteries = df.from_dict(community_batteries).div(1000)

        # ------------------------------ results from aggregators ------------------------------

        # read data from the tracker
        agg_demands, agg_batteries, agg_prices, agg_others, agg_debugger \
            = aggregator_tracker.extract_data()
        agg_times = agg_others[t_time]

        # convert data into panda dataframes
        df_agg_demands = df.from_dict(agg_demands).div(1000)
        df_agg_batteries = df.from_dict(agg_batteries).div(1000)
        df_agg_prices = df.from_dict(agg_prices)
        df_agg_others = df.from_dict(agg_others)

        def output_aggregator_results():

            # draw graphs
            p_demands = df_agg_demands.iloc[:, [0, df_agg_demands.columns[-1]]] \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kW)",
                            title=algorithm_full_names[pricing_method],
                            plot_data_points=True,
                            xticks=x_ticks,
                            show_figure=False
                            )
            p_prices = df_agg_prices.iloc[:, [0, df_agg_prices.columns[-1]]] \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Price (dollar)",
                            title=algorithm_full_names[pricing_method],
                            plot_data_points=True,
                            xticks=x_ticks,
                            show_figure=False
                            )
            p_batteries = df_agg_batteries.iloc[:, [0, df_agg_demands.columns[-1]]] \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Charge/Discharge (kW)",
                            title=algorithm_full_names[pricing_method],
                            plot_data_points=True,
                            xticks=x_ticks,
                            show_figure=False
                            )
            p_demands.y_range.start = 0
            p_prices.y_range.start = 0

            # data table
            df_agg_others[s_obj] = df_agg_others[p_cost] * int(obj_cost) + df_agg_others[s_penalty] * int(
                obj_inconvenience) + \
                                   df_agg_others[s_par] * int(obj_par)
            source = ColumnDataSource(df_agg_others)
            columns = [TableColumn(field=x, title=x.replace("_", " "), formatter=NumberFormatter(format="0.00"))
                       for x in df_agg_others.columns]
            data_table = DataTable(source=source, columns=columns)

            # put graphs and data table together into a row
            return [data_table, p_demands, p_batteries, p_prices]

        plots = output_aggregator_results()

        # ------------------------------ final schedules ------------------------------

        # extract data from the tracker
        agg_demands_final, agg_batteries_final, agg_prices_final, agg_others_final, agg_others_debugger \
            = aggregator_final.extract_data()

        # convert data into panda dataframes
        df_agg_demands_final = df.from_dict(agg_demands_final).div(1000)
        df_agg_batteries_final = df.from_dict(agg_batteries_final).div(1000)
        df_agg_prices_final = df.from_dict(agg_prices_final)
        df_agg_others_final = df.from_dict(agg_others_final)

        def output_final_schedules():

            # draw plots
            p_demands_final = df_agg_demands_final \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kW)",
                            title=algorithm_full_names[pricing_method],
                            plot_data_points=True,
                            xticks=x_ticks,
                            show_figure=False
                            )
            p_batteries_final = df_agg_batteries_final \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Charge/Discharge (kW)",
                            title=algorithm_full_names[pricing_method],
                            plot_data_points=True,
                            xticks=x_ticks,
                            show_figure=False
                            )
            p_prices_final = df_agg_prices_final \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Price (dollar)",
                            title=algorithm_full_names[pricing_method],
                            plot_data_points=True,
                            xticks=x_ticks,
                            show_figure=False
                            )
            p_demands_final.y_range.start = 0
            # p_batteries_final.y_range.start = -battery_power
            p_prices_final.y_range.start = 0

            # make data table
            df_agg_others_final[s_obj] = df_agg_others_final[p_cost] * int(obj_cost) + \
                                         df_agg_others_final[s_penalty] * int(obj_inconvenience) + \
                                         df_agg_others_final[s_par] * int(obj_par)
            source_final = ColumnDataSource(df_agg_others_final)
            columns_final = [TableColumn(field=x, title=x.replace("_", " "), formatter=NumberFormatter(format="0.00"))
                             for x in df_agg_others_final.columns]
            data_table_final = DataTable(source=source_final, columns=columns_final)

            # put graphs and the data table into a row
            return [data_table_final, p_demands_final, p_batteries_final, p_prices_final]

        plots_final = output_final_schedules()

        # ------------------------------ overview ------------------------------

        # organise important data together

        overview_dict = dict()

        def output_overview():

            overview_dict[s_par_init] = df_agg_others[s_par].values[0]
            overview_dict[s_par] = df_agg_others[s_par].values[-1]
            overview_dict[s_demand_max_init] = df_agg_others[s_demand_max].values[0]
            overview_dict[s_demand_max] = df_agg_others[s_demand_max].values[-1]
            overview_dict[s_demand_total] = df_agg_others[s_demand_total].values[-1]
            overview_dict[s_demand_reduction] = df_agg_others[s_demand_reduction].values[-1]
            overview_dict[p_cost_reduction] = df_agg_others[p_cost_reduction].values[-1]
            overview_dict[s_penalty] = df_agg_others[s_penalty].values[-1]
            overview_dict[p_cost] = df_agg_others[p_cost].values[-1]
            overview_dict[s_obj] = df_agg_others[s_obj].values[-1]
            overview_dict[t_pricing] = average(list(agg_times.values()))
            overview_dict[t_scheduling] = average(list(community_times.values()))

        output_overview()

        # ------------------------------ write all to CSV ------------------------------

        if print_demands:
            df_com_demands.to_csv(
                r"{}{}_{}_community_demands.csv".format(self.output_folder, self.subfolder_name, scheduling_method))
            df_agg_demands.to_csv(
                r"{}{}_{}_aggregator_demands.csv".format(self.output_folder, self.subfolder_name, pricing_method))
            df_agg_demands_final.to_csv(
                r"{}{}_{}_aggregator_demands_final.csv".format(self.output_folder, self.subfolder_name, pricing_method))

        if print_batteries:
            df_com_batteries.to_csv(
                r"{}{}_{}_community_batteries.csv".format(self.output_folder, self.subfolder_name, scheduling_method))
            df_agg_batteries.to_csv(
                r"{}{}_{}_aggregator_batteries.csv".format(self.output_folder, self.subfolder_name, pricing_method))
            df_agg_batteries_final.to_csv(
                r"{}{}_{}_aggregator_batteries_final.csv".format(self.output_folder, self.subfolder_name,
                                                                 pricing_method))
        if print_prices:
            df_agg_prices.to_csv(
                r"{}{}_{}_aggregator_prices.csv".format(self.output_folder, self.subfolder_name, pricing_method))
            df_agg_prices_final.to_csv(
                r"{}{}_{}_aggregator_prices_final.csv".format(self.output_folder, self.subfolder_name, pricing_method))

        if print_summary:
            df_agg_others.to_csv(
                r"{}{}_{}_aggregator_others.csv".format(self.output_folder, self.subfolder_name, pricing_method))
            df_agg_others_final.to_csv(
                r"{}{}_{}_aggregator_others_final.csv".format(self.output_folder, self.subfolder_name, pricing_method))
            df.from_dict([overview_dict]).to_csv(r"{}aggregator_overview.csv".format(self.output_folder))

        if print_debugger:
            df_com_debugger.to_csv(
                r"{}{}_{}_community_debugger.csv".format(self.output_folder, self.subfolder_name, scheduling_method))

        print("----------------------------------------")
        print("Data are written and graphs are painted. ")

        return plots, plots_final, overview_dict
        # return df.from_dict(overview_dict)
