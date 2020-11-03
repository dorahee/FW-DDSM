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
    for alg in algorithms.values():
        new_iteration.new(algorithm=alg, num_households=5,
                          max_demand_multiplier=maxium_demand_multiplier,
                          num_tasks_dependent=3,
                          full_flex_task_min=5, full_flex_task_max=0,
                          semi_flex_task_min=0, semi_flex_task_max=0,
                          fixed_task_min=0, fixed_task_max=0,
                          inconvenience_cost_weight=1, max_care_factor=care_f_max,
                          data_folder=output_folder)
        new_iteration.begin_iteration()
        new_iteration.finalise_schedules(num_samples=5)
        print("----------------------------------------")

    new_iteration.aggregator.tracker.write_to_csv(write_to_folder=output_folder,
                                                  write_to_parent_folder=output_parent_folder,
                                                  tracker_name="aggregator")
    new_iteration.aggregator.final.write_to_csv(write_to_folder=output_folder,
                                                write_to_parent_folder=output_parent_folder,
                                                tracker_name="final")
    new_iteration.community.tracker.write_to_csv(write_to_folder=output_folder,
                                                 write_to_parent_folder=output_parent_folder,
                                                 tracker_name="community",
                                                 print_demands=False, print_prices=False)

    # print("------------------------------")
    # show.write_to_csv()
    print("Experiment is finished. ")


if __name__ == '__main__':
    freeze_support()
    main()
