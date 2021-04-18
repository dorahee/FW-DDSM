include "globals.mzn";

% parameters
int: num_intervals;
set of int: INTERVALS = 1 .. num_intervals; 
	int: num_intervals_hour = num_intervals / 24;

int: num_tasks;
set of int: TASKS = 1..num_tasks;
array[TASKS] of int: durations;
array[TASKS] of int: demands;

array [TASKS, INTERVALS] of int: run_costs;

int: num_precedences;
set of int: PREC = 1..num_precedences;
array[PREC] of TASKS: predecessors;
array[PREC] of TASKS: successors;
array[PREC] of int: prec_delays;
int: max_demand;
% int: max_demand = ceil(max(demands) * 1.1);

int: capacity;
int: power;
int: init_energy;

% variables
array[INTERVALS] of var float: battery_profile;
array[INTERVALS] of var float: battery_soc;
array[TASKS] of var INTERVALS: actual_starts;
var int: obj= sum (d in TASKS) (run_costs[d, actual_starts[d]]);

% battery power rate constraint
constraint forall (i in INTERVALS) (
    battery_profile[i] <= power /\ battery_profile[i] >= -power
);

% battery capacity constraint
constraint forall (i in INTERVALS) (
    battery_soc[i] <= capacity /\ battery_soc[i] >= 0
);

% battery soc constraint
constraint battery_soc[0] = init_energy;
constraint battery_soc[num_intervals] = init_energy;
constraint forall (i in 1..num_intervals) (
    battery_soc[i] = battery_soc[i - 1] + battery_profile[i] / num_intervals_hour
);

% predecessor constraint
constraint forall (p in PREC) (
    let { TASKS: pre = predecessors[p] ;
           TASKS: succ = successors[p] ;
           int: d = prec_delays[p]; } in
     actual_starts[pre] + durations[pre] <= actual_starts[succ] 
     /\ actual_starts[succ] <= actual_starts[pre] + durations[pre] + d
   ); 

% resource capacity constraint   
constraint cumulative(actual_starts, durations, demands, max_demand);

% solve 
% :: int_search(actual_starts, first_fail, indomain_median, complete)
% minimize obj;

% output [show(actual_starts) ++ "\n" ++ show(obj)];


% array [TASKS, INTERVALS] of float: run_costs;

% solve minimize sum (d in TASKS) (run_costs[d, actual_starts[d]]);

% constraint forall (d in TASKS) (earliest_starts[d] <= actual_starts[d] /\ actual_starts[d] + durations[d] - 1 <= latest_ends[d]);
