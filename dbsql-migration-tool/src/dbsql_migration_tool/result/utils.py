import polars as pl


schema_overrides = {
    "compute": pl.Struct(
        {
            "type": pl.String,
            "cluster": pl.String,
            "warehouse_id": pl.String
        }
    ),
    "query_source": pl.Struct(
        {
            "job_info": pl.Struct(
                {
                    "job_id": pl.String,
                    "job_run_id": pl.String,
                }
            ),
            "legacy_dashboard_id": pl.String,
            "dashboard_id": pl.String,
            "alert_id": pl.String,
            "notebook_id": pl.String,
            "sql_query_id": pl.String,
            "genie_space_id": pl.String,
        }
    ),
}
