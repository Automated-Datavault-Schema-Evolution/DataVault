import os
import pandas as pd
from config import METADATA_LINEAGE_TYPE, METADATA_LINEAGE_PATH

def write_lineage(metadata_dict):
    row = pd.DataFrame([metadata_dict])
    path = METADATA_LINEAGE_PATH
    if os.path.exists(path):
        df = pd.read_parquet(path)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row
    df.to_parquet(path, index=False)