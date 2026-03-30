"""dbt profiles helper utilities.

Extracted from the original main.py without behavioral changes.
"""

import os
from pathlib import Path
from logger import log
from config import DBT_PROFILES_DIR

DEFAULT_PROFILES_YML = """default:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: "{{ env_var('THRIFT_HOST', 'localhost') }}"
      port: "{{ env_var('THRIFT_PORT', '10000') | int }}"
      user: "{{ env_var('USER', 'dbt') }}"
      schema: "{{ env_var('RAW_VAULT_SCHEMA', 'raw_vault') }}"
      connect_retries: 5
      connect_timeout: 10
      retries: 3
      threads: 4

      session_properties:
        spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
        spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        spark.sql.sources.default: "delta"
"""

def ensure_profiles_dir():
    """
    Ensure dbt profiles directory exists in the project and create a default profiles.yml if needed.
    """
    # TODO: fix the creation of the file if not exists, to create the "real" one, if not shipped
    if not os.path.exists(DBT_PROFILES_DIR):
        os.makedirs(DBT_PROFILES_DIR, exist_ok=True)
        log.info(f'[DVH_HELPER][INFO] Created dbt profiles directory: {DBT_PROFILES_DIR}')

    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        with open(profiles_yml_path, "w") as f:
            f.write("# Insert your dbt profile config here\n")
        log.info(f'[DVH_HELPER][INFO] Created empty profiles.yml at: {profiles_yml_path}')

def ensure_real_profiles():
    """
    Force DBT_PROFILES_DIR to the repo's profiles/ folder.
    Will override any attempt to create empty profiles.
    """
    # Repo root (assuming app runs in /app)
    repo_profiles = Path(__file__).resolve().parent.parent / "profiles"
    if not repo_profiles.exists():
        raise RuntimeError(f"profiles/ directory not found at {repo_profiles}")
    os.environ["DBT_PROFILES_DIR"] = str(repo_profiles)
    return repo_profiles

