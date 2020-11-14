from multiprocessing import freeze_support
from fw_ddsm.iteration import *
from fw_ddsm.output import *
from pandas import DataFrame

algorithms = dict()
algorithms[m_minizinc] = dict()
algorithms[m_minizinc][m_before_fw] = m_minizinc
algorithms[m_minizinc][m_after_fw] = f"{m_minizinc}_fw"
algorithms[m_ogsa] = dict()
algorithms[m_ogsa][m_before_fw] = m_ogsa
algorithms[m_ogsa][m_after_fw] = f"{m_ogsa}_fw"

# num_households_range = [20]
# penalty_weight_range = [0, 5, 50, 500, 5000, 50000]
# num_tasks_dependent_range = [0, 3, 5]
num_households_range = [10]
penalty_weight_range = [1, 3]
num_tasks_dependent_range = [2]
num_full_flex_tasks = 5
num_semi_flex_tasks = 0
num_fixed_tasks = 0
num_samples = 5
num_repeat = 1


def main(output_parent_folder=None, folder_id=1):
    experiment_tracker = dict()
    out = Output(output_root_folder="results", output_parent_folder=output_parent_folder)

    num_experiment = -1
    for r in range(num_repeat):
        for num_households in num_households_range:
            new_data = True

            for penalty_weight in penalty_weight_range:

                for num_tasks_dependent in num_tasks_dependent_range:

                    print("----------------------------------------")
                    print(f"{num_households} households, "
                          f"{num_tasks_dependent} dependent tasks, "
                          f"{num_full_flex_tasks} fully flexible tasks, "
                          f"{num_semi_flex_tasks} semi-flexible tasks, "
                          f"{num_fixed_tasks} fixed tasks, "
                          f"{penalty_weight} penalty weight. ")
                    print("----------------------------------------")

                    new_iteration = Iteration()
                    output_folder = out.new_output_folder(num_households=num_households,
                                                          num_dependent_tasks=num_tasks_dependent,
                                                          num_full_flex_task_min=num_full_flex_tasks,
                                                          num_semi_flex_task_min=num_semi_flex_tasks,
                                                          inconvenience_cost_weight=penalty_weight,
                                                          repeat=r, folder_id=folder_id)
                    plot_layout = []
                    plot_final_layout = []
                    for alg in algorithms.values():
                        num_experiment += 1
                        experiment_tracker[num_experiment] = dict()
                        experiment_tracker[num_experiment][k_households_no] = num_households
                        experiment_tracker[num_experiment][k_penalty_weight] = penalty_weight
                        experiment_tracker[num_experiment][k_dependent_tasks_no] = num_tasks_dependent
                        experiment_tracker[num_experiment][m_algorithm] = alg[m_after_fw]
                        experiment_tracker[num_experiment]["repeat"] = r

                        if new_data:
                            preferred_demand_profile, prices = \
                                new_iteration.new(algorithm=alg, num_households=num_households,
                                                  max_demand_multiplier=maxium_demand_multiplier,
                                                  num_tasks_dependent=num_tasks_dependent,
                                                  full_flex_task_min=num_full_flex_tasks, full_flex_task_max=0,
                                                  semi_flex_task_min=num_semi_flex_tasks, semi_flex_task_max=0,
                                                  fixed_task_min=num_fixed_tasks, fixed_task_max=0,
                                                  inconvenience_cost_weight=penalty_weight,
                                                  max_care_factor=care_f_max,
                                                  data_folder=out.output_parent_folder,
                                                  date_time=out.this_date_time)
                            # new_data = False
                        else:
                            preferred_demand_profile, prices = \
                                new_iteration.read(algorithm=alg, inconvenience_cost_weight=penalty_weight,
                                                   num_dependent_tasks=num_tasks_dependent,
                                                   read_from_folder=out.output_parent_folder,
                                                   date_time=out.this_date_time)
                        start_time_probability = new_iteration.begin_iteration(starting_prices=prices, num_cpus=4)
                        new_iteration.finalise_schedules(num_samples=num_samples,
                                                         start_time_probability=start_time_probability)
                        print("----------------------------------------")

                        plots, plots_final, overview_dict \
                            = out.save_to_output_folder(algorithm=alg,
                                                        aggregator_tracker=new_iteration.aggregator.tracker,
                                                        aggregator_final=new_iteration.aggregator.final,
                                                        community_tracker=new_iteration.community.tracker,
                                                        community_final=new_iteration.community.final)
                        experiment_tracker[num_experiment].update(overview_dict)
                        plot_layout.append(plots)
                        plot_final_layout.append(plots_final)
                        DataFrame.from_dict(experiment_tracker).transpose() \
                            .to_csv(r"{}{}_overview.csv".format(out.output_parent_folder, out.this_date_time))
                        with open(f"{out.output_parent_folder}data/{out.this_date_time}_{file_experiment_pkl}",
                                  'wb+') as f:
                            pickle.dump(experiment_tracker, f, pickle.HIGHEST_PROTOCOL)
                        print("----------------------------------------")

                    # experiment_tracker[num_experiment].update(overview_dt)
                    output_file(f"{output_folder}plots.html")
                    tab1 = Panel(child=layout(plot_layout), title="FW-DDSM results")
                    tab2 = Panel(child=layout(plot_final_layout), title="Actual schedules")
                    save(Tabs(tabs=[tab2, tab1]))
                    print("----------------------------------------")

    # print("------------------------------")
    print("Experiment is finished. ")
    print(experiment_tracker)

    # plots_experiment = []
    # df_experiments = DataFrame.from_dict(experiment_tracker).transpose()
    # df_scheduling_times = df_experiments[k_households_no, t_scheduling, k_penalty_weight, k_dependent_tasks_no]
    # df_pricing_times = df_experiments[k_households_no, t_pricing, k_penalty_weight, k_dependent_tasks_no]
    # df_par = df_experiments[k_households_no, s_par_init, s_par, k_penalty_weight, k_dependent_tasks_no]
    # df_demand_reductions = df_experiments[k_households_no, s_demand_reduction, k_penalty_weight, k_dependent_tasks_no]
    # df_cost_reductions = df_experiments[k_households_no, p_cost_reduction, k_penalty_weight, k_dependent_tasks_no]

    # time and reductions per number of household
    # time and reduction per care factors (same number of households)
    # time and reduction per number of dependent tasks (same number of households and same care factor)


if __name__ == '__main__':
    freeze_support()
    main()
