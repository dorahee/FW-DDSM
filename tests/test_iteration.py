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


def main():
    new_iteration = Iteration()
    show = Show()
    output_parent_folder, output_folder = show.set_output_folder(output_root_folder="results")
    new_data = True
    for alg in algorithms.values():
        if new_data:
            new_iteration.new(algorithm=alg, num_households=5,
                              max_demand_multiplier=maxium_demand_multiplier,
                              num_tasks_dependent=3,
                              full_flex_task_min=5, full_flex_task_max=0,
                              semi_flex_task_min=0, semi_flex_task_max=0,
                              fixed_task_min=0, fixed_task_max=0,
                              inconvenience_cost_weight=1, max_care_factor=care_f_max,
                              data_folder=output_folder)
            new_data = False
        else:
            new_iteration.read_data(algorithm=alg)
        new_iteration.begin_iteration()
        new_iteration.finalise_schedules(num_samples=5)
        print("----------------------------------------")

    show.set_data(algorithms=algorithms,
        aggregator_tracker= new_iteration.aggregator.tracker,
                  aggregator_final=new_iteration.aggregator.final,
                  community_tracker=new_iteration.community.tracker,
                  community_final=new_iteration.community.final)

    # print("------------------------------")
    # show.write_to_csv()
    print("Experiment is finished. ")


if __name__ == '__main__':
    freeze_support()
    main()
