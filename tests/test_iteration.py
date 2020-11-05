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
num_households_range = [20]
penalty_weight_range = [50, 5000]
num_tasks_dependent_range = [3, 5]
num_full_flex_tasks = 2
num_semi_flex_tasks = 3
num_fixed_tasks = 0
num_samples = 5

experiment_tracker = dict()


def main():
    show = Show(output_root_folder="results")

    num_experiment = -1
    for num_households in num_households_range:
        new_data = True

        for penalty_weight in penalty_weight_range:

            for num_tasks_dependent in num_tasks_dependent_range:
                num_experiment += 1
                experiment_tracker[num_experiment] = dict()
                key_parameters = dict()
                key_parameters[k0_households_no] = num_households
                key_parameters[k0_penalty_weight] = penalty_weight
                key_parameters[k0_num_dependent_tasks] = num_tasks_dependent
                print("----------------------------------------")
                print(f"{num_households} households, "
                      f"{num_tasks_dependent} dependent tasks, "
                      f"{num_full_flex_tasks} fully flexible tasks, "
                      f"{num_semi_flex_tasks} semi-flexible tasks, "
                      f"{num_fixed_tasks} fixed tasks, "
                      f"{penalty_weight} penalty weight. ")
                print("----------------------------------------")

                new_iteration = Iteration()
                output_folder = show.set_output_folder(num_households=num_households,
                                                       num_dependent_tasks=num_tasks_dependent,
                                                       num_full_flex_task_min=num_full_flex_tasks,
                                                       num_semi_flex_task_min=num_semi_flex_tasks,
                                                       inconvenience_cost_weight=penalty_weight)
                for alg in algorithms.values():
                    if new_data:
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
                        new_iteration.read_data(algorithm=alg)
                    new_iteration.begin_iteration()
                    new_iteration.finalise_schedules(num_samples=num_samples)
                    print("----------------------------------------")

                overview_df = show.save_data_to_files(algorithms=algorithms,
                                                      key_parameters=key_parameters,
                                                      aggregator_tracker=new_iteration.aggregator.tracker,
                                                      aggregator_final=new_iteration.aggregator.final,
                                                      community_tracker=new_iteration.community.tracker,
                                                      community_final=new_iteration.community.final)
                # experiment_tracker[num_experiment].update(overview_dt)
                print("----------------------------------------")

    # print("------------------------------")
    # show.write_to_csv()
    DataFrame.from_dict(experiment_tracker).to_csv(r"{}overview_all_tests.csv".format(show.output_parent_folder))
    print("Experiment is finished. ")


if __name__ == '__main__':
    freeze_support()
    main()
