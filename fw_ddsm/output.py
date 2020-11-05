from pathlib import Path
from pandas import DataFrame as df
from datetime import date, datetime
from pandas_bokeh import *
from bokeh.layouts import layout
from bokeh.models import ColumnDataSource, DataTable, TableColumn, Panel, Tabs, NumberFormatter
from fw_ddsm.tracker import *

class Show:

    def __init__(self, output_root_folder="results/"):

        self.output_folder = ""
        self.aggregator_tracker = Tracker()
        self.aggregator_final = Tracker()
        self.community_tracker = Tracker()
        self.community_final = Tracker()

        if not output_root_folder.endswith("/"):
            output_root_folder += "/"
        self.output_root_folder = output_root_folder

        this_date = str(date.today())
        this_time = str(datetime.now().time().strftime("%H-%M-%S"))
        self.output_parent_folder = f"{self.output_root_folder}{this_date}-{this_time}/"

    def set_output_folder(self,
                          num_households=no_households,
                          inconvenience_cost_weight=care_f_weight,
                          num_dependent_tasks=no_tasks_dependent,
                          num_full_flex_task_min=no_full_flex_tasks_min,
                          num_semi_flex_task_min=no_semi_flex_tasks_min):

        self.output_folder \
            = f"{self.output_parent_folder}/h{num_households}-w{inconvenience_cost_weight}-dt{num_dependent_tasks}" \
            f"-fft{num_full_flex_task_min}-sft{num_semi_flex_task_min}/"
        path = Path(self.output_folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)
        return self.output_folder

    def save_data_to_files(self, algorithms, key_parameters,
                           aggregator_tracker, community_tracker,
                           aggregator_final, community_final=None,
                           print_demands=False, print_prices=False, print_summary=True):
        self.aggregator_tracker = aggregator_tracker
        self.aggregator_final = aggregator_final
        self.community_tracker = community_tracker
        self.community_final = community_final

        agg_demands, agg_prices, agg_others, agg_times = aggregator_tracker.extract_data()
        agg_demands_final, agg_prices_final, agg_others_final, agg_times_final = aggregator_final.extract_data()
        community_demands, community_prices, community_others, community_times = community_tracker.extract_data()

        plot_layout = []
        plot_final_layout = []
        x_ticks = [0] + [i for i in range(1, 48) if (i + 1) % 12 == 0 ]
        # x_tick_labels = [0] + [f"{int((i + 1)/2)}h" for i in range(1, 48) if (i + 1) % 12 == 0 ]
        overview_dict = dict()
        for alg in algorithms.values():
            scheduling_method = alg[k2_before_fw]
            pricing_method = alg[k2_after_fw]
            overview_dict[pricing_method] = dict()
            overview_dict[pricing_method].update(key_parameters)

            # ------------------------------ FW results ------------------------------
            df_demands = df.from_dict(agg_demands[pricing_method]).div(1000)
            df_prices = df.from_dict(agg_prices[pricing_method])
            df_others = df.from_dict(agg_others[pricing_method])
            overview_dict[pricing_method][k0_par] = df_others[k0_par].values[-1]
            overview_dict[pricing_method][k0_demand_max] = df_others[k0_demand_max].values[-1]
            overview_dict[pricing_method][k0_demand_reduction] = df_others[k0_demand_reduction].values[-1]
            overview_dict[pricing_method][k0_cost_reduction] = df_others[k0_cost_reduction].values[-1]
            overview_dict[pricing_method][k1_time_pricing] = average(list(agg_times[pricing_method].values()))
            overview_dict[pricing_method][k1_time_scheduling] = average(list(community_times[scheduling_method].values()))

            # draw graphs
            p_demands = df_demands.iloc[:, [0, df_demands.columns[-1]]] \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kWh)",
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

            # data table
            source = ColumnDataSource(df_others)
            columns = [TableColumn(field=x, title=x.replace("_", " "), formatter=NumberFormatter(format="0.00"))
                       for x in df_others.columns]
            data_table = DataTable(source=source, columns=columns)
            plot_layout.append([p_demands, p_prices, data_table])

            # ------------------------------ final schedules ------------------------------
            df_demands_final = df.from_dict(agg_demands_final[pricing_method]).div(1000)
            df_prices_final = df.from_dict(agg_prices_final[pricing_method])
            df_others_final = df.from_dict(agg_others_final[pricing_method])

            p_demands_final = df_demands_final \
                .plot_bokeh(kind="line", xlabel="Time period", ylabel="Demand (kWh)",
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

            # data table
            source_final = ColumnDataSource(df_others_final)
            columns_final = [TableColumn(field=x, title=x.replace("_", " "), formatter=NumberFormatter(format="0.00"))
                       for x in df_others_final.columns]
            data_table_final = DataTable(source=source_final, columns=columns_final)
            plot_final_layout.append([p_demands_final, p_prices_final, data_table_final])

            # ------------------------------ write all to CSV ------------------------------
            if print_demands:
                df_demands.to_csv(r"{}{}_aggregator_demands.csv".format(self.output_folder, pricing_method))
                df_demands_final.to_csv(r"{}{}_aggregator_demands_final.csv".format(self.output_folder, pricing_method))
            if print_prices:
                df_prices.to_csv(r"{}{}_aggregator_prices.csv".format(self.output_folder, pricing_method))
                df_prices_final.to_csv(r"{}{}_aggregator_prices_final.csv".format(self.output_folder, pricing_method))
            if print_summary:
                df_others.to_csv(r"{}{}_aggregator_others.csv".format(self.output_folder, pricing_method))
                df_others_final.to_csv(r"{}{}_aggregator_others_final.csv".format(self.output_folder, pricing_method))
                df.from_dict(overview_dict).to_csv(r"{}aggregator_overview.csv".format(self.output_folder))

        agg_times.update(community_times)
        df_times = df.from_dict(agg_times)
        df_times.to_csv(r"{}run_times.csv".format(self.output_folder))

        output_file(f"{self.output_folder}plots.html")
        tab1 = Panel(child=layout(plot_layout), title="FW-DDSM results")
        tab2 = Panel(child=layout(plot_final_layout), title="Actual schedules")
        # show(Tabs(tabs=[tab1, tab2]))
        save(Tabs(tabs=[tab2, tab1]))

        print("Data are written and graphs are painted. ")
        return df.from_dict(overview_dict)

