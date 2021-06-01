include "globals.mzn";

% time
int: num_intervals;
int: num_intervals_hour; 
set of int: INTERVALS = 1..num_intervals; 

% battery
float: min_energy_capacity;  % in kwh
float: max_energy_capacity;  % in kwh
float: max_power;  % in kw

% task
int: num_tasks;
set of int: TASKS = 1..num_tasks;
array[TASKS] of int: preferred_starts;
array[TASKS] of int: earliest_starts;
array[TASKS] of int: latest_ends; 
array[TASKS] of int: durations;
array[TASKS] of int: demands;
array[TASKS] of int: care_factors;

int: num_precedences;
set of int: PREC = 1..num_precedences;
array[PREC] of TASKS: predecessors;
array[PREC] of TASKS: successors;
array[PREC] of int: prec_delays;

% price related
array[INTERVALS] of float: prices;
int: max_demand;

% decision variables
array[INTERVALS] of var min_energy_capacity..max_energy_capacity: battery_soc;  % stored energy per interval in kwh
array[INTERVALS] of var -max_power..max_power: battery_profile;  % charge/discharge per interval in kw
array[TASKS, INTERVALS] of var 0..1: actual_starts;
array[TASKS] of var INTERVALS: actual_starts2 = [sum([actual_starts[d, i] * i | i in INTERVALS]) | d in TASKS];

array[INTERVALS] of var float: total_demand_interval = [
  sum(
    [ demands[d] | d in TASKS where actual_starts2[d] <= i /\ actual_starts2[d] < actual_starts2[d] + durations[d] ]
  ) | i in INTERVALS
];

% profile constraint
% constraint forall (i in INTERVALS) (
%   battery_profile[i] >= -max_power /\ battery_profile[i] <= max_power
% );

% soc constraint
% constraint forall (i in INTERVALS) (
%   battery_soc[i] >= min_energy_capacity /\ battery_soc[i] <= max_energy_capacity
% );

constraint battery_soc[1] = max_energy_capacity;
constraint battery_soc[num_intervals] = max_energy_capacity;
constraint forall (i in 2..num_intervals) (
  battery_soc[i] = battery_soc[i - 1] + battery_profile[i] / num_intervals_hour
);

% start time window constraints
constraint forall (d in TASKS) (earliest_starts[d] <= sum(s in INTERVALS) (actual_starts[d,s] * s) /\ sum(s in INTERVALS) (actual_starts[d,s] * s) + durations[d] - 1 <= latest_ends[d]);

% predecessor constraint
constraint forall (p in PREC) (
    let { TASKS: pre = predecessors[p] ;
           TASKS: succ = successors[p] ;
           int: d = prec_delays[p]; } in
     sum(s in INTERVALS) (actual_starts[pre,s] * s) + durations[pre] <= sum(s in INTERVALS) (actual_starts[succ,s] * s) /\ 
     sum(s in INTERVALS) (actual_starts[succ,s] * s) <= sum(s in INTERVALS) (actual_starts[pre,s] * s) + durations[pre] + d); 
     
% maximum demand constraint
% constraint forall (s in INTERVALS) (total_demand_interval[s] <= max_demand);
     

% objectives
var float: total_inconvenience = sum(d in TASKS) (abs(actual_starts2[d] - preferred_starts[d]) * care_factors[d]);
var float: total_cost = sum(i in INTERVALS) ((total_demand_interval[i] + battery_profile[i]) * prices[i]) / num_intervals_hour;
var float: obj = total_inconvenience + total_cost;

solve minimize obj;