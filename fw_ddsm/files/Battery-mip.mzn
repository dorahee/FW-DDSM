
% time
int: num_intervals;
int: num_intervals_hour; 
set of int: INTERVALS = 1..num_intervals; 

% battery
float: min_energy_capacity;  % in kwh
float: max_energy_capacity;  % in kwh
float: max_power;  % in kw

% loads
array[INTERVALS] of float: existing_demands;

% price related
array[INTERVALS] of float: prices;

% decision variables
array[INTERVALS] of var min_energy_capacity..max_energy_capacity: battery_soc;  % stored energy per interval in kwh
array[INTERVALS] of var -max_power..max_power: battery_profile;  % charge/discharge per interval in kw

% soc constraint

constraint battery_soc[1] = max_energy_capacity;

constraint battery_soc[num_intervals] = max_energy_capacity;

constraint forall (i in 2..num_intervals) (
  battery_soc[i] = battery_soc[i - 1] + battery_profile[i] / num_intervals_hour
);

% total demand constraint
constraint forall (i in INTERVALS) (
  existing_demands[i] + battery_profile[i] >= 0
);

% objectives
var float: battery_cost = sum (i in INTERVALS) (battery_profile[i] * prices[i]) / num_intervals_hour;
var float: battery_degradation = 0.001 * sum(i in INTERVALS) (battery_profile[i]);
var float: obj = battery_cost + battery_degradation;

solve minimize obj;