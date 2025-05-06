
import os
import pandas as pd 
import numpy as np
import baseflow 

from tqdm import tqdm

from utils import *
from constants import *

def compute_indices(df): 
    indices = {}
    indices['F0'] = no_flow_probability(df)
    Q1, Q5, Q90, Q95 = flow_magnitude(df, [0.01, 0.05, 0.9, 0.95])
    indices['Q1'] = Q1 
    indices['Q5'] = Q5 
    indices['Q90'] = Q90 
    indices['Q95'] = Q95 
    D = no_flow_duration(df)
    meanD = np.mean(D)
    medianD = np.quantile(D, 0.5)
    sdD = np.std(D)
    indices['meanD'] = meanD 
    indices['medianD'] = medianD 
    indices['sdD'] = sdD
    D80 = no_flow_duration_rp(df, 0.8)
    indices['D80'] = D80
    Freq = no_flow_frequency(df)
    meanN = Freq.n.mean()
    medianN = Freq.n.median()
    sdN = Freq.n.std()
    indices['meanN'] = meanN 
    indices['medianN'] = medianN 
    indices['sdN'] = sdN
    theta, r = no_flow_timing(df)
    indices['theta'] = theta 
    indices['r'] = r 
    Sd6 = seasonal_predictability(df)
    indices['Sd6'] = Sd6 
    Drec = seasonal_recession_timescale(df)
    indices['Drec'] = Drec
    Ic = concavity_index(df)
    indices['Ic'] = Ic
    bfi = baseflow_index(df)
    indices['bfi'] = bfi 
    Dr = runoff_event_duration(df, n_peaks = 5)
    medianDr = np.median(Dr)
    indices['medianDr'] = medianDr
    return indices


def main():
    intermittent_metadata = pd.read_csv('intermittent_stations.csv')
    intermittent_ids = list(intermittent_metadata['ohdb_id'].unique())

    # Collect streamflow signatures 
    indices_list = []
    for i, id in enumerate(tqdm(intermittent_ids, desc="Computing streamflow signatures")):
        df = read_ohdb(id, DATADIR)
        indices = compute_indices(df)
        indices['id'] = [id]
        indices = pd.DataFrame.from_dict(indices)
        indices_list.append(indices)
    indices = pd.concat(indices_list).reset_index(drop=True)
    indices.to_csv('intermittent_stations_indices.csv', index=False)


if __name__ == '__main__':
    main()

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