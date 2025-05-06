
import pandas as pd 
import numpy as np
from functools import reduce 
from utils import *

ohdb_id = str(snakemake.wildcards['ohdb_id'])
output_filename = str(snakemake.output[0])

df = read_ohdb(ohdb_id, 'data/OHDB')
water_year_start_month = local_water_year_month(df, wettest=True)
df['water_year'] = df.apply(calculate_water_year, args=(water_year_start_month,), axis=1)
availability = df.groupby('water_year').apply(lambda x: x['Q'].notna().sum() / len(x), include_groups=False)
availability = availability[availability > 0.95]
df = df[df['water_year'].isin(availability.index)]
df.to_csv(output_filename, index=False)