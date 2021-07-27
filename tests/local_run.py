from multiprocessing import freeze_support
import sys
from fw_ddsm.iteration import *
from fw_ddsm.output import *
from pandas import DataFrame
import os
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib

algorithms = dict()
algorithms[m_minizinc] = dict()
algorithms[m_minizinc][m_before_fw] = m_minizinc
algorithms[m_minizinc][m_after_fw] = f"{m_minizinc}_fw"
# algorithms[m_ogsa] = dict()
# algorithms[m_ogsa][m_before_fw] = m_ogsa
# algorithms[m_ogsa][m_after_fw] = f"{m_ogsa}_fw"

num_households_range = [20]
penalty_weight_range = [100]
par_weight_range = [1]

num_tasks_dependent_range = [3]
num_full_flex_tasks = 0
num_semi_flex_tasks = 5
num_fixed_tasks = 3
num_samples = 5
num_repeat = 5
id_job = 0

# battery_usages = [True, False]
battery_usages = [True]
battery_solver_choice = "gurobi"
battery_fully_charged_hour = 0
battery_max_capacity_rate = 5000
battery_min_capacity_rate = 0
battery_power_rate = 5000
battery_sizes = [0, 2000, 4000, 6000, 8000, 10000]
# battery_sizes = [6000, 8000, 10000]
# battery_sizes = [4000]
# battery_efficiencies = [1, 0.99, 0.97, 0.95, 0.9, 0.75, 0.5]
battery_efficiencies = [1]

read_from_date_time = None
# read_from_date_time = "2021-06-12_20-14-46"
# read_from_date_time = "2021-06-13_17-06-35"
# read_from_date_time = "2021-06-25_03-17-35"
# read_from_date_time = "2021-07-24_18-01-32"
name_exp = None

cpus_nums = 128
# cpus_nums = cpu_count()
ensure_dependent = True
experiment_tracker = dict()
timeout = 6000
timeout = 60
min_step_size = 0.001
min_obj_improve = 1
roundup_tiny_step = False
# roundup_tiny_step = True
print_done = False
print_steps = False
# print_steps = True
email_results = True


def main(num_households, num_tasks_dependent, penalty_weight, par_weight,
         out, new_data=True, num_cpus=None, job_id=0,
         use_battery=False, capacity_max=battery_capacity_max, capacity_min=battery_capacity_min,
         power=battery_power, efficiency=battery_efficiency,
         hour_fully_charge=fully_charge_hour, read_from_dt=read_from_date_time,
         min_obj_incr=min_obj_improve):
    num_experiment = 0

    def print_summary():
        print("----------------------------------------")
        params = f"{num_households} households, " \
                 f"{capacity_max * int(use_battery)}Wh battery " \
                 f"(fully charged at {hour_fully_charge}, efficiency {efficiency}), " \
                 f"{num_tasks_dependent} dependent tasks, " \
                 f"{num_full_flex_tasks} fully flexible tasks, " \
                 f"{num_semi_flex_tasks} semi-flexible tasks, " \
                 f"{num_fixed_tasks} fixed tasks, " \
                 f"{penalty_weight} penalty weight, " \
                 f"{par_weight} par weight, " \
                 f"read from {read_from_dt}. "
        print(params)
        print("----------------------------------------")
        return params

    param_str = print_summary()

    new_iteration = Iteration()
    output_folder, output_parent_folder, this_date_time \
        = out.new_output_folder(num_households=num_households,
                                num_dependent_tasks=num_tasks_dependent,
                                num_full_flex_task_min=num_full_flex_tasks,
                                num_semi_flex_task_min=num_semi_flex_tasks,
                                par_cost_weight=par_weight,
                                inconvenience_cost_weight=penalty_weight,
                                folder_id=job_id,
                                battery_size=int(use_battery) * capacity_max,
                                efficiency=efficiency)

    plots_demand_layout = []
    plots_demand_finalised_layout = []
    for alg in algorithms.values():
        while num_experiment in experiment_tracker:
            num_experiment += 1

        def record_experiment_tracker():
            experiment_tracker[num_experiment] = dict()
            experiment_tracker[num_experiment][k_households_no] = num_households
            experiment_tracker[num_experiment][k_penalty_weight] = penalty_weight
            experiment_tracker[num_experiment][p_par_weight] = par_weight
            experiment_tracker[num_experiment][k_dependent_tasks_no] = num_tasks_dependent
            experiment_tracker[num_experiment][h_tasks_no_ff_min] = num_full_flex_tasks
            experiment_tracker[num_experiment][h_tasks_no_sf_min] = num_semi_flex_tasks
            experiment_tracker[num_experiment][h_tasks_no_fixed_min] = num_fixed_tasks
            experiment_tracker[num_experiment][m_algorithm] = alg[m_after_fw]
            experiment_tracker[num_experiment]["id"] = job_id

            if use_battery:
                experiment_tracker[num_experiment][b_cap_max] = capacity_max
                experiment_tracker[num_experiment][b_cap_min] = capacity_min
                experiment_tracker[num_experiment][b_power] = power
                experiment_tracker[num_experiment][b_eff] = efficiency

        record_experiment_tracker()

        # 1. iteration data
        if new_data:
            preferred_demand_profile, prices = \
                new_iteration.new(algorithm=alg, num_households=num_households,
                                  num_intervals=no_intervals,
                                  max_demand_multiplier=maximum_demand_multiplier,
                                  num_tasks_dependent=num_tasks_dependent, ensure_dependent=ensure_dependent,
                                  full_flex_task_min=num_full_flex_tasks, full_flex_task_max=0,
                                  semi_flex_task_min=num_semi_flex_tasks, semi_flex_task_max=0,
                                  fixed_task_min=num_fixed_tasks, fixed_task_max=0,
                                  inconvenience_cost_weight=penalty_weight,
                                  par_cost_weight=par_weight,
                                  max_care_factor=care_f_max,
                                  data_folder=output_parent_folder,
                                  backup_data_folder=output_folder,
                                  date_time=this_date_time,
                                  use_battery=use_battery,
                                  battery_model=file_mip_battery, battery_solver="gurobi",
                                  timeout=time_out,
                                  fully_charge_time=fully_charge_hour,
                                  capacity_max=capacity_max, capacity_min=capacity_min,
                                  power=power, efficiency=efficiency)
            new_data = False
        else:
            if m_ogsa in alg or new_data is False:
                num_tasks_dependent = None
                print("Same dependent tasks. ")
                print("----------------------------------------")

            if read_from_dt is not None:
                input_date_time = read_from_dt
                intput_parent_folder = out1.output_root_folder + input_date_time
            else:
                input_date_time = this_date_time
                intput_parent_folder = output_parent_folder
            preferred_demand_profile, prices = \
                new_iteration.read(algorithm=alg,
                                   num_intervals=no_intervals,
                                   inconvenience_cost_weight=penalty_weight,
                                   par_cost_weight=par_weight,
                                   new_dependent_tasks=num_tasks_dependent,
                                   ensure_dependent=ensure_dependent,
                                   read_from_folder=intput_parent_folder,
                                   date_time=input_date_time,
                                   use_battery=use_battery,
                                   battery_model=file_mip_battery, battery_solver="gurobi",
                                   timeout=time_out,
                                   fully_charge_time=fully_charge_hour,
                                   capacity_max=capacity_max, capacity_min=capacity_min,
                                   power=power, efficiency=efficiency)

        # 2. iteration begins
        start_time_probability, num_iterations = \
            new_iteration.begin_iteration(starting_prices=prices,
                                          par_cost_weight=par_weight,
                                          use_battery=use_battery,
                                          battery_solver=battery_solver_choice,
                                          num_cpus=num_cpus,
                                          timeout=timeout,
                                          fully_charge_time=hour_fully_charge,
                                          min_step_size=min_step_size,
                                          roundup_tiny_step=roundup_tiny_step,
                                          print_done=print_done, print_steps=print_steps,
                                          min_obj_incr=min_obj_incr)
        experiment_tracker[num_experiment][k_iteration_no] = num_iterations

        # 3. finalising schedules
        new_iteration.finalise_schedules(num_samples=num_samples,
                                         par_cost_weight=par_weight,
                                         start_time_probability=start_time_probability)

        # 4. preparing plots and writing results to CSVs
        plots_demand, plots_demand_finalised, overview_dict \
            = out.save_to_output_folder(algorithm=alg,
                                        aggregator_tracker=new_iteration.aggregator.tracker,
                                        aggregator_final=new_iteration.aggregator.final,
                                        community_tracker=new_iteration.community.tracker,
                                        community_final=new_iteration.community.final,
                                        obj_par=True)
        plots_demand_layout.append(plots_demand)
        plots_demand_finalised_layout.append(plots_demand_finalised)
        experiment_tracker[num_experiment].update(overview_dict)

    # 5. drawing all plots
    print("----------------------------------------")
    output_file(f"{output_folder}{this_date_time}_plots.html")
    tab1 = Panel(child=layout(plots_demand_layout), title="FW-DDSM results")
    tab2 = Panel(child=layout(plots_demand_finalised_layout), title="Actual schedules")
    div = Div(text=f"""{param_str}""", width=1600)
    save(layout([div], [Tabs(tabs=[tab1, tab2])]))

    # 6. writing experiment overview
    df_exp = DataFrame.from_dict(experiment_tracker).transpose()
    df_exp.to_csv(r"{}{}_overview.csv".format(output_parent_folder, this_date_time))

    print("----------------------------------------")
    print("Experiment is finished. ")
    print(df_exp[[k_households_no, k_dependent_tasks_no, k_penalty_weight, p_par_weight,
                  m_algorithm, k_iteration_no, s_par_init, s_par,
                  s_demand_reduction, p_cost_reduction]])


if __name__ == '__main__':
    freeze_support()
    print(f"Arguments count: {len(sys.argv)}")

    # try:
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")

        if i == 1:
            cpus_nums = int(arg)
        elif i == 2:
            num_households_range = [int(arg)]
        elif i == 3:
            penalty_weight_range = [int(arg)]
        elif i == 4:
            num_tasks_dependent_range = [int(arg)]
        elif i == 5:
            id_job = int(arg)
        elif i == 6:
            name_exp = str(arg)

    out1 = Output(output_root_folder="results", output_parent_folder=name_exp)

    for r in range(num_repeat):
        for h in num_households_range:
            new = True
            if read_from_date_time is not None:
                new = False
            for w in penalty_weight_range:
                for w2 in par_weight_range:
                    for dt in num_tasks_dependent_range:
                        for battery_use in battery_usages:
                            for battery_size in battery_sizes:
                                for battery_efficiency in battery_efficiencies:
                                    main(new_data=new,
                                         num_households=h,
                                         num_tasks_dependent=dt,
                                         penalty_weight=w,
                                         par_weight=w2,
                                         out=out1,
                                         num_cpus=cpus_nums,
                                         job_id=r,
                                         use_battery=battery_use,
                                         capacity_max=battery_size,
                                         capacity_min=battery_min_capacity_rate,
                                         power=battery_size,
                                         efficiency=battery_efficiency,
                                         hour_fully_charge=battery_fully_charged_hour,
                                         read_from_dt=read_from_date_time)
                                    new = False
    # except Exception as e:
    #     print(e.args)
    #     print()
