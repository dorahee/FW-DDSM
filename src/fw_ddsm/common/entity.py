from tracker import *
import pickle5 as pickle
from pathlib import Path


class Entity:

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        self.times = {
            k_no_intervals: num_intervals,
            k_no_periods: num_periods,
            k_no_intervals_periods: int(no_intervals / no_periods),
            k_time_out: time_out
        }

        self.data_dict = dict()
        self.tracker = Tracker()
        self.final = Tracker()


class Demand(Entity):

    def __init__(self, num_intervals=no_intervals, num_periods=no_periods):
        super().__init__(num_intervals, num_periods)

        self.preferred_demand_profile = []
        self.tasks_scheduling_method = ""

        self.input_files = {
            k_file_preferred_demand_profile: "",
            k_file_list_of_devices_power: "",
            k_init_preferred_demand_profile: (),
            k_list_of_devices_power: ()
        }

        self.task_params = {
            k_min_full_flex_tasks: min_full_flex_tasks,
            k_max_full_flex_tasks: max_full_flex_tasks,
            k_min_semi_flex_tasks: min_semi_flex_tasks,
            k_max_semi_flex_tasks: max_semi_flex_tasks,
            k_min_fixed_tasks: min_fixed_tasks,
            k_max_fixed_tasks: max_fixed_tasks,
            k_no_dependent_tasks: no_dependent_tasks,
            k_ensure_dependent: ensure_dependent
        }

        self.battery_params = {
            k_max_battery_capacity: max_battery_capacity,
            k_min_battery_capacity: min_battery_capacity,
            k_battery_power: battery_power,
            k_fully_charge_hour: fully_charge_hour,
            k_battery_efficiency: battery_efficiency
        }

        self.weights = {
            k_inconvenience_weight: inconvenience_weight,
            k_par_weight: par_weight,
        }

        self.multipliers = {
            k_max_care_f: max_care_f,
            k_max_demand_multiplier: maximum_demand_multiplier
        }

        self.date_time = ""
        self.write_to_folder = ""

    def set_parameters(self,
                       num_intervals, tasks_scheduling_method,
                       write_to_folder, date_time,

                       # input files
                       preferred_demand_profile, list_of_devices_power,
                       file_preferred_demand_profile, file_list_of_devices_power,

                       # task params
                       min_full_flex_task, max_full_flex_task,
                       min_semi_flex_task, max_semi_flex_task,
                       min_fixed_task, max_fixed_task,
                       num_tasks_dependent, ensure_dependent,

                       # battery params
                       capacity_max, capacity_min, power, efficiency,

                       # objective weights
                       par_weight, inconvenience_cost_weight,

                       # multipliers
                       max_care_factor, max_demand_multiplier):

        self.times[k_no_intervals] = num_intervals
        self.tasks_scheduling_method = tasks_scheduling_method
        self.date_time = date_time
        self.write_to_folder = write_to_folder

        self.input_files[k_init_preferred_demand_profile] = preferred_demand_profile
        self.input_files[k_list_of_devices_power] = list_of_devices_power
        self.input_files[k_file_preferred_demand_profile] = file_preferred_demand_profile
        self.input_files[k_file_list_of_devices_power] = file_list_of_devices_power

        self.task_params[k_min_full_flex_tasks] = min_full_flex_task
        self.task_params[k_max_full_flex_tasks] = max_full_flex_task
        self.task_params[k_min_semi_flex_tasks] = min_semi_flex_task
        self.task_params[k_max_semi_flex_tasks] = max_semi_flex_task
        self.task_params[k_no_dependent_tasks] = num_tasks_dependent
        self.task_params[k_ensure_dependent] = ensure_dependent

        self.battery_params[k_max_battery_capacity] = capacity_max
        self.battery_params[k_min_battery_capacity] = capacity_min
        self.battery_params[k_battery_power] = power
        self.battery_params[k_battery_efficiency] = efficiency

        self.weights[k_par_weight] = par_weight
        self.weights[k_inconvenience_weight] = inconvenience_cost_weight

        self.multipliers[k_max_care_f] = max_care_factor
        self.multipliers[k_max_demand_multiplier] = max_demand_multiplier

    def convert_price(self, num_intervals, prices):
        num_periods = len(prices)
        num_intervals_period = int(num_intervals / num_periods)
        if num_periods != num_intervals:
            prices = [p for p in prices for _ in range(num_intervals_period)]
        else:
            prices = [p for p in prices]

        return prices

    def save_to_file(self, file_name, folder):

        if not folder.endswith("/"):
            folder += "/"
        folder += "data/"

        path = Path(folder)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        self.data_dict[s_demand] = self.preferred_demand_profile
        with open(file_name, 'wb+') as f:
            pickle.dump(self.data_dict, f, pickle.HIGHEST_PROTOCOL)
        del self.data_dict[s_demand]
        f.close()

        # if date_time is None:
        #     file_name = f"{folder}{file_pkl}"
        # else:
        #     file_name = f"{folder}{date_time}_{file_pkl}"
