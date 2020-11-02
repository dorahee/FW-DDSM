from fw_ddsm.aggregator import *
from fw_ddsm.parameter import *

test_aggregator = Aggregator()
test_aggregator.new(normalised_pricing_table_csv=file_pricing_table,
                    aggregate_preferred_demand_profile=[5] * no_periods,
                    pricing_method=f"{k1_ogsa}_fw",
                    weight=pricing_table_weight,
                    write_to_file_path="aggregator")
test_aggregator.new(normalised_pricing_table_csv=file_pricing_table,
                    aggregate_preferred_demand_profile=[5] * no_periods,
                    pricing_method=f"{k1_minizinc}_fw",
                    weight=pricing_table_weight,
                    write_to_file_path="aggregator")
print()
