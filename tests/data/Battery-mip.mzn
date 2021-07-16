
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
array[INTERVALS] of var min_energy_capacity..max_energy_capacity: battery_soc;  % stored energy per interval in wh
array[INTERVALS] of var 0..max_power: battery_charge;  % charge/discharge per interval in w
array[INTERVALS] of var -max_power..0: battery_discharge;  % charge/discharge per interval in w

% array[INTERVALS] of var -max_power..max_power: battery_profile;  % charge/discharge per interval in kw
array[INTERVALS] of var 0..total_demand_limit: total_demand = array1d([existing_demands[i] + battery_charge[i] / efficiency + battery_discharge[i] * efficiency | i in INTERVALS]);  % charge/discharge per interval in kw

% additional variables for PAR minimisation
var float: max_total_demand; 
var float: z;
array[INTERVALS] of var 0..total_demand_limit: total_demand2 = array1d([z * total_demand[i] | i in INTERVALS]); 


% non-shedding constraint
% constraint battery_soc[1] <= battery_soc[num_intervals];

% only charge or discharge at each time interval
constraint sum(i in INTERVALS)(battery_charge[i] * battery_discharge[i]) = 0;

% soc constraints
% constraint battery_soc[1] <= max_energy_capacity;
% constraint battery_soc[num_intervals] <= max_energy_capacity;
constraint forall (i in 2..num_intervals) (
  battery_soc[i] * num_intervals_hour = battery_soc[i - 1] * num_intervals_hour + battery_charge[i - 1] + battery_discharge[i - 1]
);

constraint battery_soc[1] * num_intervals_hour = battery_soc[num_intervals] * num_intervals_hour + battery_charge[num_intervals] + battery_discharge[num_intervals];

% PAR linearisation constraints
constraint forall(i in INTERVALS) (max_total_demand >= total_demand[i]);
constraint forall (i in INTERVALS) (max_total_demand >= total_demand2[i]);
constraint sum(total_demand2) = sum(total_demand) * z;
constraint sum(total_demand2) = num_intervals;

% objectives
var float: cost = sum (i in INTERVALS) (total_demand[i] * prices[i]);
var float: obj = cost + 10 * max_total_demand;
solve minimize obj;

% output [
% "obj = " ++ show(obj) ++ "\n" ++
% "existing_demand = " ++ show(existing_demands) ++ "\n" ++
% "total_demand = " ++ show(total_demand) ++ "\n" ++
% "total_demand2 = " ++ show(total_demand2) ++ "\n" ++
% "R = " ++ show(R) ++ "\n" ++
% "battery_profile = " ++ show(battery_profile)
% ];
