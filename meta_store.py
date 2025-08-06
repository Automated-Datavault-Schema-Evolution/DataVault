import os
import pandas as pd
from config import METADATA_LINEAGE_PATH, METADATA_METADATA_PATH


def _append_parquet(row, path):
    if os.path.exists(path):
        df = pd.read_parquet(path)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row
    df.to_parquet(path, index=False)


def write_lineage(metadata_dict):
    """Append lineage information to the lineage metastore."""
    _append_parquet(pd.DataFrame([metadata_dict]), METADATA_LINEAGE_PATH)


def write_metadata(metadata_dict):
    """Append model metadata to the metadata metastore."""
    _append_parquet(pd.DataFrame([metadata_dict]), METADATA_METADATA_PATH)