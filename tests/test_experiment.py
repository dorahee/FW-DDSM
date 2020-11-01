from fw_ddsm.experiment import *
from fw_ddsm import output
from multiprocessing import freeze_support

algorithms = dict()
# algorithms[k1_minizinc] = dict()
# algorithms[k1_minizinc][k2_before_fw] = k1_minizinc
# algorithms[k1_minizinc][k2_after_fw] = f"{k1_minizinc}_fw"
algorithms[k1_ogsa] = dict()
algorithms[k1_ogsa][k2_before_fw] = k1_ogsa
algorithms[k1_ogsa][k2_after_fw] = f"{k1_ogsa}_fw"


def main():
    show = output.Show(output_folder="results")
    new_experiment = Experiment(algorithms=algorithms, num_households=20)
    new_experiment.new_data(max_demand_multiplier=maxium_demand_multiplier,
                            num_tasks_dependent=3,
                            full_flex_task_min=5, full_flex_task_max=0,
                            semi_flex_task_min=0, semi_flex_task_max=0,
                            fixed_task_min=0, fixed_task_max=0,
                            inconvenience_cost_weight=1, max_care_factor=care_f_max,
                            data_folder=show.output_folder)
    # new_experiment.read_data()

    for alg in algorithms.values():
        new_experiment.iteration(alg)
        new_experiment.final_schedule(alg)
        show.set_data(aggregator_data=new_experiment.aggregator.aggregator,
                      community_aggregate=new_experiment.community.aggregate_data,
                      algorithm=alg)
        print("------------------------------")
        show.write_to_csv()
    print("Experiment is finished. ")


if __name__ == '__main__':
    freeze_support()
    main()

# main()
