from multiprocessing import freeze_support
from fw_ddsm.iteration import *
from fw_ddsm.output import *
from pandas import DataFrame

algorithms = dict()
algorithms[k1_minizinc] = dict()
algorithms[k1_minizinc][k2_before_fw] = k1_minizinc
algorithms[k1_minizinc][k2_after_fw] = f"{k1_minizinc}_fw"
algorithms[k1_ogsa] = dict()
algorithms[k1_ogsa][k2_before_fw] = k1_ogsa
algorithms[k1_ogsa][k2_after_fw] = f"{k1_ogsa}_fw"

# num_households_range = [20]
# penalty_weight_range = [0, 5, 50, 500, 5000, 50000]
# num_tasks_dependent_range = [0, 3, 5]
num_households_range = [10, 20]
penalty_weight_range = [5, 500]
num_tasks_dependent_range = [1, 3]
num_full_flex_tasks = 5
num_semi_flex_tasks = 0
num_fixed_tasks = 0
num_samples = 5

experiment_tracker = dict()


def main():
    out = Output(output_root_folder="results")

    num_experiment = -1
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
                                                      inconvenience_cost_weight=penalty_weight)
                plot_layout = []
                plot_final_layout = []
                for alg in algorithms.values():
                    num_experiment += 1
                    experiment_tracker[num_experiment] = dict()
                    experiment_tracker[num_experiment][k0_households_no] = num_households
                    experiment_tracker[num_experiment][k0_penalty_weight] = penalty_weight
                    experiment_tracker[num_experiment][k0_num_dependent_tasks] = num_tasks_dependent
                    experiment_tracker[num_experiment][k0_algorithm] = alg[k2_after_fw]

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
                                              data_folder=output_folder)
                        new_data = False
                    else:
                        preferred_demand_profile, prices = \
                            new_iteration.read(algorithm=alg)
                    start_time_probability = new_iteration.begin_iteration(starting_prices=prices)
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
                        .to_csv(r"{}overview_all_tests.csv".format(out.output_parent_folder))
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
    # time and reductions per number of household
    # time and reduction per care factors (same number of households)
    # time and reduction per number of dependent tasks (same number of households and same care factor)


if __name__ == '__main__':
    freeze_support()
    main()
