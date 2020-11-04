from multiprocessing import freeze_support
from fw_ddsm.iteration import *
from fw_ddsm.output import *

algorithms = dict()
algorithms[k1_minizinc] = dict()
algorithms[k1_minizinc][k2_before_fw] = k1_minizinc
algorithms[k1_minizinc][k2_after_fw] = f"{k1_minizinc}_fw"
algorithms[k1_ogsa] = dict()
algorithms[k1_ogsa][k2_before_fw] = k1_ogsa
algorithms[k1_ogsa][k2_after_fw] = f"{k1_ogsa}_fw"

num_households_range = [200, 50, 80, 100]
penalty_weight = 1
num_tasks_dependent = 2
num_full_flex_tasks = 5
num_semi_flex_tasks = 0
num_fixed_tasks = 0

def main():
    show = Show(output_root_folder="results")
    for num_households in num_households_range:
        print("----------------------------------------")
        print(f"{num_households} households, {num_tasks_dependent} dependent tasks, {penalty_weight} penalty weight. ")
        print("----------------------------------------")
        new_iteration = Iteration()
        output_folder = show.set_output_folder(num_households=num_households,
                                               num_dependent_tasks=num_tasks_dependent,
                                               inconvenience_cost_weight=penalty_weight)
        new_data = True
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
            new_iteration.finalise_schedules(num_samples=5)
            print("----------------------------------------")

        show.save_data_to_files(algorithms=algorithms,
                                aggregator_tracker=new_iteration.aggregator.tracker,
                                aggregator_final=new_iteration.aggregator.final,
                                community_tracker=new_iteration.community.tracker,
                                community_final=new_iteration.community.final)
        print("----------------------------------------")

    # print("------------------------------")
    # show.write_to_csv()
    print("Experiment is finished. ")


if __name__ == '__main__':
    freeze_support()
    main()
