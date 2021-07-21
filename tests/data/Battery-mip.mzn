
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
array[INTERVALS] of var 0..max_power: battery_charge;  % charge/discharge per interval in w
array[INTERVALS] of var -max_power..0: battery_discharge;  % charge/discharge per interval in w
array[INTERVALS] of var min_energy_capacity..max_energy_capacity: battery_soc;  % stored energy per interval in wh

array[INTERVALS] of var 0..total_demand_limit: total_demand = array1d([existing_demands[i] + battery_charge[i] / efficiency + battery_discharge[i] * efficiency | i in INTERVALS]);  % charge/discharge per interval in kw

% additional variables for PAR linearisation
var float: max_total_demand; 
% var float: z;
% array[INTERVALS] of var 0..total_demand_limit: total_demand2 = array1d([total_demand[i] * z | i in INTERVALS]);  % charge/discharge per interval in kw


% PAR linearisation constraints
constraint forall(i in INTERVALS) (max_total_demand >= total_demand[i]);
% constraint sum(total_demand2) = num_intervals;

% non-shedding constraint
constraint sum(i in INTERVALS)(battery_charge[i] + battery_discharge[i]) = 0;
% constraint sum(i in INTERVALS)(battery_charge2[i] + battery_discharge2[i]) = 0;

% only charge or discharge at each time interval
constraint sum(i in INTERVALS)(battery_charge[i] * battery_discharge[i]) = 0;

% soc constraints
constraint forall (i in 2..num_intervals) (
  battery_soc[i] * num_intervals_hour = battery_soc[i - 1] * num_intervals_hour 
  + battery_charge[i - 1] + battery_discharge[i - 1]
);

constraint battery_soc[1] * num_intervals_hour = battery_soc[num_intervals] * num_intervals_hour + battery_charge[num_intervals] + battery_discharge[num_intervals];

% objectives
var float: cost = sum (i in INTERVALS) (total_demand[i] * prices[i]);
var float: obj = cost + 10 * max_total_demand;
solve minimize obj;

% output [
% "obj = " ++ show(obj) ++ "\n"
% ++ "cost = " ++ show(cost) ++ "\n"
% ++ "existing_demand = " ++ show(existing_demands) ++ "\n"
% ++ "total_demand = " ++ show(total_demand) ++ "\n"
% ];
