import logging

import polars as pl

from pathlib import Path
from sqlalchemy import create_engine, text

from dbsql_migration_tool.common.config import SourceOptions


logger = logging.getLogger(name=__name__)


def read_queries(config: SourceOptions) -> pl.DataFrame:
    """Reads a query set for a specific workload from trino/starburst."""
    logger.info("retrieving queries from %s ...", config.path)

    path = Path(config.path)
    data = [
        {
            "query_id": x.name.split(".", maxsplit=1)[0],
            "query": x.read_text()
        } for x in path.glob("**/*.sql") if x.is_file()
    ]

    df = pl.DataFrame(data)
    logger.info("retrieved %d queries ...", df.shape[0])
    return df


def write_sql_script(query_id: str, query: str, path: str) -> bool:
    """Writes a SQL string to a SQL file."""
    p = Path(path)
    try:
        p.mkdir(parents=True, exist_ok=True)
        (p / (f"{query_id}.sql")).write_text(query.lstrip().rstrip())
    except Exception as ex:
        logger.error(ex)
        return False
    return True


def read_from_uc(connection_url: str, catalog: str, schema: str, table: str, schema_overrides: dict | None = None, query: str | None = None) -> pl.DataFrame:
    """Reads a table from Unity-Catalog."""

    catalog = catalog.replace("-", "_")
    schema = schema.replace("-", "_")
    table = table.replace("-", "_")

    engine = create_engine(
        url=f"{connection_url}&catalog={catalog}&schema={schema}"
    )
    connection = engine.connect()

    df = pl.read_database(
        query=query if query else f"SELECT * FROM {table}",
        connection=connection,
        schema_overrides=schema_overrides,
    )

    connection.close()
    engine.dispose()
    return df


def write_to_uc(df: pl.DataFrame, connection_url: str, catalog: str, schema: str, table: str) -> None:
    """Writes a table to Unity-Catalog."""
    catalog = catalog.replace("-", "_")
    schema = schema.replace("-", "_")
    table = table.replace("-", "_")
    engine = create_engine(
        url=f"{connection_url}&catalog={catalog}&schema={schema}",
    )
    connection = engine.connect()
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    df.to_pandas().to_sql(table, con=connection, if_exists="replace", index=False)
    connection.close()
    engine.dispose()

