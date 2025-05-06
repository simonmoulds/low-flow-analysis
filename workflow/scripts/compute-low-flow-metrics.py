
import pandas as pd 
import numpy as np
from functools import reduce 
from pathlib import Path

from hydrots.timeseries import HydroTS

# metric = str(snakemake.wildcards['metric'])
input_filename = str(snakemake.input[0])
output_filename = str(snakemake.output[0])

# # TESTING 
# import importlib 
# import workflow.scripts.utils as utils
# importlib.reload(utils)
# metric = 'mam7d'
# input_filename = 'results/robin_processed/AU00101.csv'

df = pd.read_csv(input_filename, parse_dates=['date'])
ts = HydroTS(df, metadata=None, freq='1D')
ts.update_validity_criteria(min_tot_years=30, min_availability=0.9)
ts.update_intermittency_criteria(min_zero_flow_days=5, min_zero_flow_years=1)
if ts.is_valid:
    amax = ts.summary.annual_maximum_flow()
    amax.to_csv(output_filename, index=True)
# else:
#     Path(output_filename).touch()

# ts.summary.annual_maximum_flood() 
# min7 = ts.summary.n_day_low_flow_extreme(n=7)
# min30 = ts.summary.n_day_low_flow_extreme(n=30)
# max7 = ts.summary.n_day_high_flow_extreme(n=7)
# min7dur = ts.summary.max_low_flow_duration(0.99)
# max_deficit = ts.summary.max_low_flow_deficit(0.5)
# noflow_freq = ts.summary.no_flow_frequency()
# noflow_dur = ts.summary.no_flow_event_duration()

# # This shows how we can dynamically register a new summary function:
# def annual_mean_flow(ts): 
#     return ts.data.groupby('water_year')['Q'].mean()
# ts.summary.register_summary_function('annual_mean_flow', annual_mean_flow)
# ts.summary.annual_mean_flow()
# df = pd.read_csv(input_filename, parse_dates=['date'])
# ts = HydroTS(df, metadata=None, freq='1D')
# ts.update_validity_criteria(min_tot_years=30, min_availability=0.9)
# ts.update_intermittency_criteria(min_zero_flow_days=5, min_zero_flow_years=1)
# if ts.is_valid and not ts.is_intermittent: 
#     amax = ts.summary.annual_maximum_flood()




# df = assign_water_year(df)
# df = df[df.groupby('water_year').transform('size') >= 365].reset_index(drop=True)
# is_intermittent(df)
# result = n_day_low_flow_extreme(df, n=7)
# result.to_csv(output_filename)

# result = n_day_low_flow_extreme(df, n=14)
# result = max_low_flow_duration(df, quantile=0.95)
# result = max_low_flow_duration(df, quantile=0.90)
# result = max_low_flow_deficit(df, quantile=0.95)
# result = max_low_flow_deficit(df, quantile=0.90)
# result = baseflow_index_decadal(df)

# bfi = bfi.rename({'Q': 'Qb'}, axis=1)
# df = pd.merge(df, bfi, left_index=True, right_index=True).reset_index()
# # Make sure 'date' is in datetime format
# df['date'] = pd.to_datetime(df['date'])
# # Plotting the data
# plt.figure(figsize=(10, 6))
# # Plot Total Flow
# plt.plot(df['date'], df['Q'], label='Total Flow', color='b', linewidth=2)
# # Plot Baseflow
# plt.plot(df['date'], df['Qb'], label='Baseflow', color='g', linewidth=2)
# # Add titles and labels
# plt.title('Total Flow and Baseflow Over Time', fontsize=16)
# plt.xlabel('Date', fontsize=12)
# plt.ylabel('Flow (m³/s)', fontsize=12)
# # Display the legend
# plt.legend()
# # Display the plot
# plt.grid(True)
# plt.show()