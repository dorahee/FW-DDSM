from pandas import read_csv
import pickle
from pathlib import Path
from fw_ddsm.cfunctions import *
from fw_ddsm.parameter import *



def new_pricing_table(normalised_pricing_table_csv, maximum_demand_level, weight=pricing_table_weight,
                      num_periods=no_periods):
    # ---------------------------------------------------------------------- #
    # normalised_pricing_table_csv:
    #       the path of the CSV file of the normalised pricing table
    # demand_level_scalar:
    #       the scalar for rescaling the normalised demand levels
    # ---------------------------------------------------------------------- #

    csv_table = read_csv(normalised_pricing_table_csv, header=None)
    num_levels = len(csv_table.index)
    demand_level_scalar = maximum_demand_level * weight
    csv_table.loc[num_levels + 1] = [csv_table[0].values[-1] * 10] + [demand_level_scalar * 1.2 for _ in range(num_periods)]

    zero_digit = 2
    pricing_table = dict()
    pricing_table[k0_price_levels] = list(csv_table[0].values)
    pricing_table[k0_demand_table] = dict()
    pricing_table[k0_demand_table] = \
        {period:
             {level:
                  round(csv_table[period + 1].values[level] * demand_level_scalar, -zero_digit)
              for level in range(len(csv_table[period + 1]))}
         for period in range(num_periods)}

    return pricing_table



def new_aggregate(aggregate_preferred_demand_profile, algorithms_options,
                  write_to_file_path=None):
    max_demand = max(aggregate_preferred_demand_profile)
    total_demand = sum(aggregate_preferred_demand_profile)
    par = round(max_demand / average(aggregate_preferred_demand_profile), 2)

    aggregator = dict()
    aggregator[k0_demand_max] = max_demand
    aggregator[k0_demand] = aggregate_preferred_demand_profile
    for algorithm in algorithms_options.values():
        for alg in algorithm.values():
            aggregator[alg] = dict()
            aggregator[alg][k0_demand] = dict()
            aggregator[alg][k0_demand_max] = dict()
            aggregator[alg][k0_demand_total] = dict()
            aggregator[alg][k0_par] = dict()
            aggregator[alg][k0_penalty] = dict()
            aggregator[alg][k0_final] = dict()
            aggregator[alg][k0_prices] = dict()
            aggregator[alg][k0_cost] = dict()
            aggregator[alg][k0_step] = dict()

            aggregator[alg][k0_demand][0] = aggregate_preferred_demand_profile
            aggregator[alg][k0_demand_max][0] = max_demand
            aggregator[alg][k0_demand_total][0] = total_demand
            aggregator[alg][k0_par][0] = par
            aggregator[alg][k0_par][0] = par
            aggregator[alg][k0_penalty][0] =None
            aggregator[alg][k0_cost][0] = None
            aggregator[alg][k0_step][0] = 1

    if write_to_file_path is not None:
        write_to_file_path = write_to_file_path if write_to_file_path.endswith("/") \
            else write_to_file_path + "/"
        path = Path(write_to_file_path)
        if not path.exists():
            path.mkdir(mode=0o777, parents=True, exist_ok=False)

        with open(f"{write_to_file_path}aggregator.pkl", 'wb+') as f:
            pickle.dump(aggregator, f, pickle.HIGHEST_PROTOCOL)
        f.close()

    return aggregator




def existing_aggregator(file_path):
    # ---------------------------------------------------------------------- #
    # ---------------------------------------------------------------------- #

    file_path = file_path if file_path.endswith("/") else file_path + "/"

    with open(file_path + "aggregator" + '.pkl', 'rb') as f:
        aggregator = pickle.load(f)
    f.close()

    return aggregator


