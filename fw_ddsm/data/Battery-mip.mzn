
% time
int: num_intervals;
int: num_intervals_hour; 
set of int: INTERVALS = 1..num_intervals; 

% battery
float: min_energy_capacity;  % in kwh
float: max_energy_capacity;  % in kwh
float: max_power;  % in kw
float: efficiency;
int: fully_charge_hour;

% demands
array[INTERVALS] of float: existing_demands;
float: total_demand_limit = 9999999999.9;

% price related
array[INTERVALS] of float: prices;

% decision variables for the battery scheduling problem
array[INTERVALS] of var min_energy_capacity..max_energy_capacity: battery_soc;  % stored energy per interval in kwh
array[INTERVALS] of var -max_power..max_power: battery_profile;  % charge/discharge per interval in kw
array[INTERVALS] of var 0..total_demand_limit: total_demand = array1d([existing_demands[i] + battery_profile[i] | i in INTERVALS]);  % charge/discharge per interval in kw

% additional variables for PAR minimisation
var float: max_total_demand; 
var float: R;
array[INTERVALS] of var 0..total_demand_limit: total_demand2 = array1d([R * (existing_demands[i] + battery_profile[i]) | i in INTERVALS]); 


% full circle constraint
constraint sum(battery_profile) = 0;

% soc constraints
constraint battery_soc[1] = max_energy_capacity;
constraint battery_soc[num_intervals] = max_energy_capacity;
constraint forall (i in 2..num_intervals) (
  battery_soc[i] = battery_soc[i - 1] + battery_profile[i - 1] / num_intervals_hour
);

% PAR linearisation constraints
constraint forall(i in INTERVALS) (max_total_demand >= total_demand[i]);
constraint sum(total_demand2) = sum(total_demand) * R;
constraint sum(total_demand2) = num_intervals;
constraint forall (i in INTERVALS) (max_total_demand >= total_demand2[i]);

% objectives

var float: cost = sum (i in INTERVALS) ((battery_profile[i] + existing_demands[i]) * prices[i]) / num_intervals_hour;
var float: obj = cost + max_total_demand;
solve minimize obj;

% output [
% "obj = " ++ show(obj) ++ "\n" ++
% "existing_demand = " ++ show(existing_demands) ++ "\n" ++
% "total_demand = " ++ show(total_demand) ++ "\n" ++
% "total_demand2 = " ++ show(total_demand2) ++ "\n" ++
% "R = " ++ show(R) ++ "\n" ++
% "battery_profile = " ++ show(battery_profile)
% ];