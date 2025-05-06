
import pandas as pd 
import numpy as np
from tqdm import tqdm
from utils import *


ohdb_metadata_filename = snakemake.input[0]
output_filename = snakemake.output[0]

ohdb_rootdir = 'data/OHDB'

# Load metadata and match to intermittent stations
meta = pd.read_csv(ohdb_metadata_filename)
ohdb_ids = list(meta['ohdb_id'])

# Initialize a list to keep track of intermittent files
intermittent = np.zeros(len(ohdb_ids), dtype=bool)
valid = np.zeros(len(ohdb_ids), dtype=bool)

# Progress bar
for i, id in enumerate(tqdm(ohdb_ids, desc="Processing files")):
    # Load the CSV data
    df = read_ohdb(id, ohdb_rootdir)
    if is_valid(df): 
        valid[i] = True 
        if is_intermittent(df):
            intermittent[i] = True 

meta['valid_for_trend_analysis'] = valid
meta['intermittent'] = intermittent 

# Write intermittent metadata to CSV
meta.to_csv(output_filename, index=False)
