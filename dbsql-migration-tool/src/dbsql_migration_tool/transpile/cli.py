import os
import click
import polars as pl
import logging

from typing import Any
from databricks.sql import connect

from dbsql_migration_tool.common.config import Config
from dbsql_migration_tool.common.utils import unnest_with_prefix, transform_df
from dbsql_migration_tool.transpile.udf import _transpile_query_udf, _validate_query_udf
from dbsql_migration_tool.common.io import read_queries, write_sql_script, write_to_uc


logger = logging.getLogger(__name__)


def _write_scripts(df: pl.DataFrame, query_col: str, path: str) -> pl.DataFrame:
    """Writes SQL queries in a DF to SQL files."""
    written = df.with_columns(
        pl.struct("query_id", query_col).map_elements(
            lambda row: write_sql_script(
                query_id=row["query_id"],
                query=row[query_col],
                path=path,
            ),
            return_dtype=pl.Boolean()
        ).alias("written")
    ).select("written").unpivot()["value"].any()

    if not written:
        raise RuntimeError(f"Failed to write {os.path.basename(path)} scripts!")

    return df


def _run_transpile(df: pl.DataFrame, catalog: str, from_dialect: str, to_dialect: str) -> pl.DataFrame:
    """Runs the actual transpilation logic for queries defined in a DF."""
    df = df.with_columns(
        pl.struct("query_id", "query").map_elements(
            lambda row: _transpile_query_udf(
                query_id=row["query_id"],
                query=row["query"],
                catalog=catalog,
                from_dialect=from_dialect,
                to_dialect=to_dialect,
            ),
            return_dtype=pl.Struct(
                {
                    "query": pl.String,
                    "success": pl.Boolean,
                    "msg": pl.String
                }
            )
        ).alias("transpile")
    )

    return df

def _run_validate(df: pl.DataFrame, connection: dict[str, str]) -> pl.DataFrame:
    """Runs a simple validation of transpiled queries."""
    conn = connect(
        server_hostname=connection["host"],
        http_path=connection["http_path"],
        access_token=connection["token"],
    )

    df = df.with_columns(
        pl.struct("query_id", "transpile_query").map_elements(
            lambda row: _validate_query_udf(
                query_id=row["query_id"],
                query=row["transpile_query"],
                connection=conn,
            ),
            return_dtype=pl.Struct({"success": pl.Boolean, "msg": pl.String})
        ).alias("validate")
    )
    conn.close()
    return df


def _transpile_report(df: pl.DataFrame, validate: bool) -> None:
    """Creates a simple output of overall statistics."""
    total_cnt = df.shape[0]

    transpile_success = df.filter(pl.col("transpile_success") == True).shape[0]
    transpile_error = df.filter(pl.col("transpile_success") == False).shape[0]

    print("\n---------------------")
    print("transpilation report")
    print("---------------------")
    print(f"\ntotal queries: {total_cnt}")
    print(f"\ntranspiled: {transpile_success} / {total_cnt}")
    print(f"transpile errors: {transpile_error} / {total_cnt}")
    if validate:
        validation_success = df.filter(
            (pl.col("transpile_success") == True) & (pl.col("validate_success") == True)
        ).shape[0]
        validation_error = df.filter(
            (pl.col("transpile_success") == True) & (pl.col("validate_success") == False)
        ).shape[0]
        print(f"\nvalidated: {validation_success} / {transpile_success}")
        print(f"validation errors: {validation_error} / {transpile_success}")
    print("\n---------------------")


@click.command(name="transpile")
@click.option(
    "-c",
    "--config",
    "file",
    type=click.Path(),
    required=True,
    help="A path to a config-file ['.yaml'] providing configurations."
)
def transpile(**kwargs: Any) -> None:
    """CLI entrypoint for running a transpilation."""
    config = Config(**kwargs)
    out_path = f"{config.out_path}/workload-{config.workload}-{config.name}"

    # load query set
    df = read_queries(
        config=config.source,
    )

    logger.info("transpiling queries with sqlglot ...")

    # transpile queries
    df = transform_df(
        df=df,
        func=_run_transpile,
        parallelism=config.parallelism,
        **{
            "catalog": config.target.catalog,
            "from_dialect": config.source.dialect,
            "to_dialect": config.target.dialect,
        }
    )

    df = unnest_with_prefix(df=df, column="transpile", prefix="transpile")

    # validate transpiled queries
    if config.validation:
        logger.info("validating queries ...")

        df = transform_df(
            df=df,
            func=_run_validate,
            parallelism=config.parallelism,
            **{
                "connection": {
                    "host": config.target.host,
                    "http_path": config.target.http_path,
                    "token": config.target.token,
                },
            }
        )

        df = unnest_with_prefix(df=df, column="validate", prefix="validate")

    # persist queries in separate SQL scripts
    if config.persist_queries:
        logger.info("persisting queries ...")

        transform_df(
            df=df,
            func=_write_scripts,
            parallelism=config.parallelism,
            **{
                "query_col": "query",
                "path": f"{out_path}/scripts/{config.source.dialect}",
            }
        )

        transform_df(
            df=df.filter(pl.col("transpile_success") == True),
            func=_write_scripts,
            parallelism=config.parallelism,
            **{
                "query_col": "transpile_query",
                "path": f"{out_path}/scripts/{config.target.dialect}"
            }
        )

    # persisting results
    write_to_uc(
        df=df,
        connection_url=config.target.connection_url,
        catalog=config.results.catalog,
        schema=f"workload-{config.workload}-{config.name}",
        table="transpile"
    )
    _transpile_report(df=df, validate=config.validation)
