import click
import logging
import polars as pl

from typing import Any
from databricks.sql import connect

from dbsql_migration_tool.common.config import Config
from dbsql_migration_tool.common.io import read_from_uc, write_to_uc
from dbsql_migration_tool.common.utils import unnest_with_prefix, transform_df
from dbsql_migration_tool.experiment.udf import _experiment_udf

logger = logging.getLogger(name=__name__)


def _run_experiment(df: pl.DataFrame, connection: dict[str, str], runs: int) -> pl.DataFrame:
    """Runs the actual experiment."""
    conn = connect(
        server_hostname=connection["host"],
        http_path=connection["http_path"],
        access_token=connection["token"],
    )
    df = df.with_columns(
        pl.struct("query_id", "transpile_query").map_elements(
            lambda row: _experiment_udf(
                query_id=row["query_id"],
                query=row["transpile_query"],
                connection=conn,
                runs=runs,
            ),
            return_dtype=pl.List(
                pl.Struct({"run": pl.Int8, "statement_id": pl.String, "success": pl.Boolean, "msg": pl.String})
            )
        ).alias("experiment")
    )
    conn.close()
    return df

def _experiment_report(df: pl.DataFrame, query_runs: int) -> None:
    """Creates a simple output of overall statistics."""
    total_cnt = df.shape[0]
    distinct_queries = int(total_cnt / query_runs)

    experiment_success = df.filter(pl.col("experiment_success") == True).shape[0]
    experiment_error = df.filter(pl.col("experiment_success") == False).shape[0]

    print("\n---------------------")
    print("experiment report")
    print("---------------------")
    print(f"\ndistinct queries: {distinct_queries}")
    print(f"runs per query: {query_runs}")
    print(f"total runs: {total_cnt}")
    print(f"\nqueries with all runs successful: {experiment_success} / {total_cnt}")
    print(f"queries with run errors: {experiment_error} / {total_cnt}")
    print("\n---------------------")


@click.command(name="experiment")
@click.option(
    "-c",
    "--config",
    "file",
    type=click.Path(),
    required=True,
    help="A path to a config-file ['.yaml'] providing configurations."
)
def experiment(**kwargs: Any) -> None:
    """CLI entrypoint for running an experiment."""
    config = Config(**kwargs)

    logger.info("read experiment metadata from transpilation results ...")

    df = read_from_uc(
        connection_url=config.target.connection_url,
        catalog=config.results.catalog,
        schema=f"workload-{config.workload}-{config.name}",
        table="transpile"
    )
    df = df.filter(pl.col("transpile_success") == True)

    logger.info(
        "starting experiment for workload %d with %d queries, parallelism: %d, runs per query: %d ...",
        config.workload,
        df.shape[0],
        config.parallelism,
        config.query_runs,
    )

    df = transform_df(
        df=df,
        func=_run_experiment,
        parallelism=config.parallelism,
        **{
            "connection": {
                "host": config.target.host,
                "http_path": config.target.http_path,
                "token": config.target.token,
            },
            "runs": config.query_runs,
        }
    )

    df = df.explode("experiment")
    df = unnest_with_prefix(df=df, column="experiment", prefix="experiment")

    # persisting results
    write_to_uc(
        df=df,
        connection_url=config.target.connection_url,
        catalog=config.results.catalog,
        schema=f"workload-{config.workload}-{config.name}",
        table="experiment"
    )
    _experiment_report(df=df, query_runs=config.query_runs)
