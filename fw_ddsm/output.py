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
                          use_battery=False,
                          repeat=None, folder_id=None):

        self.output_folder \
            = f"{self.output_parent_folder}/h{num_households}-w{inconvenience_cost_weight}-dt{num_dependent_tasks}" \
              f"-fft{num_full_flex_task_min}-sft{num_semi_flex_task_min}-b{int(use_battery)}"
        if repeat is not None:
            self.output_folder += f"-r{repeat}"
        if folder_id is not None:
            self.output_folder += f"-id{folder_id}/"
        self.output_folder += "/"
        path = Path(self.output_folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        return self.output_folder, self.output_parent_folder, self.this_date_time

    def save_to_output_folder(self, algorithm,
                              aggregator_tracker, community_tracker,
                              aggregator_final, community_final=None,
                              params_tracker=None,
                              obj_par=False, obj_cost=True, obj_inconvenience=True,
                              print_demands=True, print_batteries=True, print_prices=True, print_summary=True):

        agg_demands, agg_batteries, agg_prices, agg_others = aggregator_tracker.extract_data()
        agg_demands_final, agg_batteries_final, agg_prices_final, agg_others_final = aggregator_final.extract_data()
        community_demands, community_batteries, community_prices, community_others = community_tracker.extract_data()

        plot_layout = []
        plot_final_layout = []
        x_ticks = [0] + [i for i in range(1, 48) if (i + 1) % 12 == 0]
        # x_tick_labels = [0] + [f"{int((i + 1)/2)}h" for i in range(1, 48) if (i + 1) % 12 == 0 ]
        overview_dict = dict()
        agg_times = agg_others[t_time]
        community_times = community_others[t_time]

        # ------------------------------ FW results ------------------------------
        df_demands = df.from_dict(agg_demands).div(1000)
        df_batteries = df.from_dict(agg_batteries).div(1000)
        df_prices = df.from_dict(agg_prices)
        df_others = df.from_dict(agg_others)
        overview_dict[s_par_init] = df_others[s_par].values[0]
        overview_dict[s_par] = df_others[s_par].values[-1]
        overview_dict[s_demand_max_init] = df_others[s_demand_max].values[0]
        overview_dict[s_demand_max] = df_others[s_demand_max].values[-1]
        overview_dict[s_demand_total] = df_others[s_demand_total].values[-1]
        overview_dict[s_demand_reduction] = df_others[s_demand_reduction].values[-1]
        overview_dict[p_cost_reduction] = df_others[p_cost_reduction].values[-1]
        overview_dict[s_penalty] = df_others[s_penalty].values[-1]
        overview_dict[p_cost] = df_others[p_cost].values[-1]
        overview_dict[s_obj] = df_others[s_obj].values[-1]
        overview_dict[t_pricing] = average(list(agg_times.values()))
        overview_dict[t_scheduling] = average(list(community_times.values()))

        scheduling_method = algorithm[m_before_fw]
        pricing_method = algorithm[m_after_fw]

        # draw graphs
        p_demands = df_demands.iloc[:, [0, df_demands.columns[-1]]] \
            .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kW)",
                        title=algorithm_full_names[pricing_method],
                        plot_data_points=True,
                        xticks=x_ticks,
                        show_figure=False
                        )
        p_prices = df_prices.iloc[:, [0, df_prices.columns[-1]]] \
            .plot_bokeh(kind="line", xlabel="Time period", ylabel="Price (dollar)",
                        title=algorithm_full_names[pricing_method],
                        plot_data_points=True,
                        xticks=x_ticks,
                        show_figure=False
                        )
        p_batteries = df_batteries.iloc[:, [0, df_demands.columns[-1]]] \
            .plot_bokeh(kind="line", xlabel="Time period", ylabel="Charge/Discharge (kW)",
                        title=algorithm_full_names[pricing_method],
                        plot_data_points=True,
                        xticks=x_ticks,
                        show_figure=False
                        )
        p_demands.y_range.start = 0
        p_prices.y_range.start = 0
        # p_batteries.y_range.start = -battery_power

        # data table
        df_others[s_obj] = df_others[p_cost] * int(obj_cost) + df_others[s_penalty] * int(obj_inconvenience) + \
                           df_others[s_par] * int(obj_par)
        source = ColumnDataSource(df_others)
        columns = [TableColumn(field=x, title=x.replace("_", " "), formatter=NumberFormatter(format="0.00"))
                   for x in df_others.columns]
        data_table = DataTable(source=source, columns=columns)
        plots = [data_table, p_demands, p_batteries, p_prices]

        # ------------------------------ final schedules ------------------------------
        df_demands_final = df.from_dict(agg_demands_final).div(1000)
        df_batteries_final = df.from_dict(agg_batteries_final).div(1000)
        df_prices_final = df.from_dict(agg_prices_final)
        df_others_final = df.from_dict(agg_others_final)

        p_demands_final = df_demands_final \
            .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kW)",
                        title=algorithm_full_names[pricing_method],
                        plot_data_points=True,
                        xticks=x_ticks,
                        show_figure=False
                        )
        p_batteries_final = df_batteries_final \
            .plot_bokeh(kind="line", xlabel="Time period", ylabel="Charge/Discharge (kW)",
                        title=algorithm_full_names[pricing_method],
                        plot_data_points=True,
                        xticks=x_ticks,
                        show_figure=False
                        )
        p_prices_final = df_prices_final \
            .plot_bokeh(kind="line", xlabel="Time period", ylabel="Price (dollar)",
                        title=algorithm_full_names[pricing_method],
                        plot_data_points=True,
                        xticks=x_ticks,
                        show_figure=False
                        )
        p_demands_final.y_range.start = 0
        # p_batteries_final.y_range.start = -battery_power
        p_prices_final.y_range.start = 0

        # data table
        df_others_final[s_obj] = df_others_final[p_cost] * int(obj_cost) + \
                                 df_others_final[s_penalty] * int(obj_inconvenience) + \
                                 df_others_final[s_par] * int(obj_par)
        source_final = ColumnDataSource(df_others_final)
        columns_final = [TableColumn(field=x, title=x.replace("_", " "), formatter=NumberFormatter(format="0.00"))
                         for x in df_others_final.columns]
        data_table_final = DataTable(source=source_final, columns=columns_final)
        plots_final = [data_table_final, p_demands_final, p_batteries_final, p_prices_final]

        # ------------------------------ write all to CSV ------------------------------

        if print_demands:
            df_demands.to_csv(r"{}{}_aggregator_demands.csv".format(self.output_folder, pricing_method))
            df_demands_final.to_csv(r"{}{}_aggregator_demands_final.csv".format(self.output_folder, pricing_method))
        if print_batteries:
            df_batteries.to_csv(r"{}{}_aggregator_batteries.csv".format(self.output_folder, pricing_method))
            df_batteries_final.to_csv(r"{}{}_aggregator_batteries_final.csv".format(self.output_folder, pricing_method))
        if print_prices:
            df_prices.to_csv(r"{}{}_aggregator_prices.csv".format(self.output_folder, pricing_method))
            df_prices_final.to_csv(r"{}{}_aggregator_prices_final.csv".format(self.output_folder, pricing_method))
        if print_summary:
            df_others.to_csv(r"{}{}_aggregator_others.csv".format(self.output_folder, pricing_method))
            df_others_final.to_csv(r"{}{}_aggregator_others_final.csv".format(self.output_folder, pricing_method))
            df.from_dict([overview_dict]).to_csv(r"{}aggregator_overview.csv".format(self.output_folder))

        print("----------------------------------------")
        print("Data are written and graphs are painted. ")

        # # ------------------------------ save input data ------------------------------
        # src = self.output_parent_folder + "/data"
        # dest = self.output_folder + "/data"
        # path = Path(dest)
        # if not path.exists():
        #     path.mkdir(mode=0o777, parents=True, exist_ok=False)
        # src_files = os.listdir(src)
        # for file_name in src_files:
        #     full_file_name = os.path.join(src, file_name)
        #     if os.path.isfile(full_file_name):
        #         shutil.copy(full_file_name, dest)
        #
        # print("----------------------------------------")
        # print("Input data are backed up. ")

        return plots, plots_final, overview_dict
        # return df.from_dict(overview_dict)
