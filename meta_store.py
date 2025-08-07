import datetime
import os
import uuid

import pandas as pd
import psycopg2
from psycopg2 import sql
import json
from config import (
    LAKE_TYPE,
    METADATA_LINEAGE_PATH,
    METADATA_METADATA_PATH,
    METASTORE_DB_HOST,
    METASTORE_DB_PORT,
    METASTORE_DB,
    METASTORE_DB_USER,
    METASTORE_DB_PASSWORD,
    METASTORE_DB_SCHEMA,
)



def _append_parquet(row, path):
    if os.path.exists(path):
        df = pd.read_parquet(path)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row
    df.to_parquet(path, index=False)

def _append_db(data, table):
    with psycopg2.connect(
            host=METASTORE_DB_HOST,
            port=METASTORE_DB_PORT,
            dbname=METASTORE_DB,
            user=METASTORE_DB_USER,
            password=METASTORE_DB_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            # Ensure target table exists.  Each record is stored as JSONB to
            # keep the schema flexible.
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        id SERIAL PRIMARY KEY,
                        payload JSONB,
                        inserted_at TIMESTAMPTZ DEFAULT NOW()
                    )
                    """
                ).format(sql.Identifier(METASTORE_DB_SCHEMA), sql.Identifier(table))
            )

            cur.execute(
                sql.SQL("INSERT INTO {}.{} (payload) VALUES (%s)").format(
                    sql.Identifier(METASTORE_DB_SCHEMA), sql.Identifier(table)
                ),
                [json.dumps(data)],
            )
        conn.commit()

def write_lineage(metadata_dict):
    """Append lineage information to the lineage metastore."""
    metadata_dict.setdefault("lineage_id", str(uuid.uuid4()))
    metadata_dict.setdefault("logged_at", datetime.utcnow().isoformat())

    if LAKE_TYPE == "rdbms":
        _append_db(metadata_dict, "lineage")
    else:
        _append_parquet(pd.DataFrame([metadata_dict]), METADATA_LINEAGE_PATH)


def write_metadata(metadata_dict):
    """Append model metadata to the metadata metastore."""
    if LAKE_TYPE == "rdbms":
        _append_db(metadata_dict, "metadata")
    else:
        _append_parquet(pd.DataFrame([metadata_dict]), METADATA_METADATA_PATH)