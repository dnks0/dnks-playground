import click
import logging
import polars as pl
from typing import Any


from dbsql_migration_tool.common.io import read_from_uc, write_to_uc
from dbsql_migration_tool.common.config import Config
from databricks_sql_poc.result.utils import schema_overrides

logger = logging.getLogger(name=__name__)


@click.command(name="result")
@click.option(
    "-c",
    "--config",
    "file",
    type=click.Path(),
    required=True,
    help="A path to a config-file ['.yaml'] providing configurations."
)
def result(**kwargs: Any) -> None:
    """CLI entrypoint for collecting results."""
    config = Config(**kwargs)

    logger.info("read experiment metadata from experiment results ...")
    df = read_from_uc(
        connection_url=config.target.connection_url,
        catalog=config.results.catalog,
        schema=f"workload-{config.workload}-{config.name}",
        table="experiment"
    )
    df = df.filter(pl.col("experiment_success") == True)

    query = f"""
    SELECT
        statement_id,
        total_duration_ms,
        waiting_for_compute_duration_ms,
        waiting_at_capacity_duration_ms,
        execution_duration_ms,
        compilation_duration_ms,
        total_task_duration_ms,
        result_fetch_duration_ms,
        read_files,
        read_rows,
        produced_rows,
        read_bytes,
        written_bytes
    FROM system.query.history
    WHERE compute.warehouse_id = '{config.target.warehouse_id}'
    """

    query_history_df = read_from_uc(
        connection_url=config.target.connection_url,
        catalog="system",
        schema="query",
        table="history",
        schema_overrides=schema_overrides,
        query=query,
    )

    joined = df.join(
        other=query_history_df,
        how="left",
        left_on="experiment_statement_id",
        right_on="statement_id",
    )

    df = joined.group_by(
        "query_id",
        "query",
    ).agg(
        pl.first("transpile_query").alias("transpile_query"),
        pl.count("experiment_run").alias("experiment_runs"),
        pl.col("experiment_statement_id").str.join(",").alias("experiment_statement_ids"),
        pl.mean("total_duration_ms").alias("avg_duration_ms"),
        pl.mean("waiting_for_compute_duration_ms").alias("avg_waiting_for_compute_duration_ms"),
        pl.mean("waiting_at_capacity_duration_ms").alias("avg_waiting_at_capacity_duration_ms"),
        pl.mean("execution_duration_ms").alias("avg_execution_duration_ms"),
        pl.mean("compilation_duration_ms").alias("avg_compilation_duration_ms"),
        pl.mean("total_task_duration_ms").alias("avg_total_task_duration_ms"),
        pl.mean("result_fetch_duration_ms").alias("avg_result_fetch_duration_ms"),
        pl.mean("read_files").alias("avg_read_files"),
        pl.mean("read_rows").alias("avg_read_rows"),
        pl.mean("produced_rows").alias("avg_produced_rows"),
        pl.mean("read_bytes").alias("avg_read_bytes"),
        pl.mean("written_bytes").alias("avg_written_bytes"),
    )

    write_to_uc(
        df=df,
        connection_url=config.target.connection_url,
        catalog=config.results.catalog,
        schema=f"workload-{config.workload}-{config.name}",
        table="result",
    )
