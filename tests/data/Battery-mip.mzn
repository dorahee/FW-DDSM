
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
float: demand_limit = 9999999999.9;

% price related
array[INTERVALS] of float: prices;

% decision variables for the battery scheduling problem

% additional variables for PAR minimisation
float: minz = 0 ;
float: maxz = 1 ;
var float: z;
%constraint z=1 ;
constraint z >= minz  /\ z <= maxz ;

array[INTERVALS] of var min_energy_capacity..max_energy_capacity: battery_soc;  % stored energy per interval in wh
array[INTERVALS] of var 0..max_power: battery_charge;  % charge/discharge per interval in w
array[INTERVALS] of var -max_power..0: battery_discharge;  % charge/discharge per interval in w

% array[INTERVALS] of var -max_power..max_power: battery_profile;  % charge/discharge per interval in kw
array[INTERVALS] of var 0..demand_limit: demand = 
array1d([existing_demands[i] 
      + (battery_charge[i]  / efficiency) 
      + (battery_discharge[i] * efficiency) 
       | i in INTERVALS]);  % charge/discharge per interval in kw

% soc constraints
constraint 
forall(i in 1..num_intervals)(battery_soc[i] <= max_energy_capacity);

constraint 
forall(i in 1..num_intervals)(battery_soc[i] >= min_energy_capacity);

constraint 
forall (i in 2..num_intervals) (
battery_soc[i] * num_intervals_hour - battery_soc[i - 1] * num_intervals_hour = 
battery_charge[i - 1] + battery_discharge[i - 1]);

constraint 
battery_soc[1] * num_intervals_hour - battery_soc[num_intervals] * num_intervals_hour = battery_charge[num_intervals] + battery_discharge[num_intervals];

array[INTERVALS] of var minz * min_energy_capacity..maxz * max_energy_capacity: battery_soc2;  % stored energy per interval in wh
array[INTERVALS] of var 0..maxz * max_power: battery_charge2;  % charge/discharge per interval in w
array[INTERVALS] of var -maxz * max_power..0: battery_discharge2;  % charge/discharge per interval in w

% array[INTERVALS] of var -max_power..max_power: battery_profile;  % charge/discharge per interval in kw
array[INTERVALS] of var 0..maxz * demand_limit: demand2 = 
array1d([z * existing_demands[i] 
      + (battery_charge2[i] / efficiency) 
      + (battery_discharge2[i] * efficiency) 
       | i in INTERVALS]);  % charge/discharge per interval in kw

% soc constraints
constraint 
forall(i in 1..num_intervals)(battery_soc2[i] <= max_energy_capacity * z);

constraint 
forall(i in 1..num_intervals)(battery_soc2[i] >= min_energy_capacity * z);

constraint 
forall (i in 2..num_intervals) (battery_soc2[i] * num_intervals_hour - battery_soc2[i - 1] * num_intervals_hour = battery_charge2[i - 1] + battery_discharge2[i - 1]);

constraint 
battery_soc2[1] * num_intervals_hour - battery_soc2[num_intervals] * num_intervals_hour = battery_charge2[num_intervals] + battery_discharge2[num_intervals];

% PAR Definition
var float: PAR =% max_demand * num_intervals / sum(demand) ;
                             %    max_demand * num_interval * z / sum(demand) * z ;
                              %    max(demand2) * num_intervals / 1 ;
                              max(demand2) * num_intervals ;
constraint sum(demand2) = 1 ; 

% objectives
var float: cost = sum (i in INTERVALS) (demand[i] * prices[i]);
var float: obj = cost + 10 * PAR ; %max_total_demand;
solve minimize obj;

% output [ "Cost = ",  show(cost), "\n",
%                "z = ", show(z), "\n",
%                "PAR = ", show(PAR), "\n",
%                "obj = ", show(obj), "\n",
%                "Max Demand = ", show(max(demand)), "\n",
%                "All Demand = ", show(demand), "\n",
%                "All Demand2 = ", show([demand2[i]/z | i in 1..num_intervals])
%                ] ;