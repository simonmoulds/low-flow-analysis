# Main entrypoint of the workflow.
# Please follow the best practices:
# https://snakemake.readthedocs.io/en/stable/snakefiles/best_practices.html,
# in particular regarding the standardized folder structure mentioned there.

import os 
import platform
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from itertools import product

current_platform = platform.system()

py_env_file = 'envs/conda_environment_base.yml'

DATADIR = Path(os.getenv('DATA'))
ROBIN_DATADIR = DATADIR / 'data' / 'robin' / '3b077711-f183-42f1-bac6-c892922c81f4'

robin_metadata = pd.read_csv(ROBIN_DATADIR / 'supporting-documents' / 'robin_station_metadata_public_v1-1.csv', encoding='latin1')

ROBIN_IDS = [id for id in robin_metadata['ROBIN_ID'] if Path(ROBIN_DATADIR / 'data' / f'{id}.csv').exists()]