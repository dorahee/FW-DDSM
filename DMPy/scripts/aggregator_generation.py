from pandas import read_csv
import pickle
from math import ceil
from more_itertools import grouper
from time import time
from fw_ddsm.parameter import *


def new_pricing_table(normalised_pricing_table_csv, maximum_demand_level,
                      num_periods=no_intervals, weight=pricing_table_weight):
    # ---------------------------------------------------------------------- #
    # normalised_pricing_table_csv:
    #       the path of the CSV file of the normalised pricing table
    # demand_level_scalar:
    #       the scalar for rescaling the normalised demand levels
    # ---------------------------------------------------------------------- #

    csv_table = read_csv(normalised_pricing_table_csv, header=None)
    num_levels = len(csv_table.index)
    demand_level_scalar = maximum_demand_level * weight
    csv_table.loc[num_levels + 1] = [csv_table[0].values[-1] * 10] + [demand_level_scalar * 1.2 for _ in
                                                                      range(num_periods)]

    zero_digit = 100
    pricing_table = dict()
    pricing_table[p_price_levels] = list(csv_table[0].values)
    pricing_table[p_demand_table] = dict()
    pricing_table[p_demand_table] = \
        {period:
             {level:
                  ceil(csv_table[period + 1].values[level] * demand_level_scalar / zero_digit) * zero_digit
              for level in range(len(csv_table[period + 1]))}
         for period in range(num_periods)}

    return pricing_table
