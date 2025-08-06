import pyspark.sql.functions as F

def model(dbt, session):
    # Data Vault {model_type} for table: {table_name}
    df = dbt.source("staging", "{table_name}")
    {builder_code}
    dbt.log("Model {model_name} produced " + str(out_df.count()) + " rows.")
    import datetime
    from meta_store import write_lineage
    write_lineage({{
        "model_name": "{model_name}",
        "table_name": "{table_name}",
        "model_type": "{model_type}",
        "columns": out_df.columns,
        "business_keys": {business_keys},
        "executed_at": datetime.datetime.now().isoformat(),
        "row_count": out_df.count(),
        "extra": {{}}
    }})
    return out_df