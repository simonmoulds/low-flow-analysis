
import pandas as pd 
import numpy as np
from pathlib import Path

robin_id = str(snakemake.wildcards['robin_id'])
input_filepath = Path(snakemake.input[0])
output_filepath = Path(snakemake.output[0])

df = pd.read_csv(input_filepath, parse_dates=['date'])
df.to_csv(output_filepath, index=False)