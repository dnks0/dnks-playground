import logging
import sqlglot
from databricks.sql.client import Connection, Error
from sqlglot import Expression, exp
from sqlglot.expressions import Table, Identifier


logger = logging.getLogger(name=__name__)


def _transpile_query_udf(
        query_id: str, query: str, from_dialect: str, to_dialect: str, catalog: str,
) -> dict[str, [str | bool]]:
    """Performs the actual transpilation of a query with sqlglot. This is done two fold:
    1. Transpile with sqlglot
    2. Manually modify specific parts of a query as necessary
    """
    try:
        result = sqlglot.transpile(
            sql=query,
            read=from_dialect,
            write=to_dialect,
            error_level=sqlglot.errors.ErrorLevel.IMMEDIATE,
            unsupported_level=sqlglot.errors.ErrorLevel.IMMEDIATE,
            pretty=True,
        )
    except (sqlglot.errors.UnsupportedError, sqlglot.errors.SqlglotError, Exception) as ex:
        logger.error("could not transpile query %s: %s", query_id, str(ex))
        transpiled_query = None
        success = False
        msg = str(ex)
    else:
        transpiled_query = result[0]
        if catalog:
            transpiled_query = _modify_query(
                query=result[0],
                catalog=catalog,
                dialect=to_dialect,
            )
        success = True
        msg = "success"

    return {"query": transpiled_query, "success": success, "msg": msg}


def _validate_query_udf(query_id: str, query: str | None, connection: Connection) -> dict[str, [str | bool]]:
    """Validates a single SQL query against a defined connection."""
    if query is None:
        success = False
        msg = "query not transpiled"
    else:
        with connection.cursor() as cursor:
            try:
                statement = f"EXPLAIN\n{query}"
                cursor.execute(statement)
                cursor.fetchone()
                # Syntax errors (e.g. invalid Databricks dialect) are raised as sql.Error
            except Error as ex:
                logger.error("could not validate query %s: %s", query_id, str(ex))
                success = True
                msg = str(ex)
                # Query errors (e.g. TABLE_NOT_FOUND) returns DBSQL exception in the resultset
            else:
                success = True
                msg = "success"

    return {"success": success, "msg": msg}


def _modify_query(query: str, catalog: str, dialect: str) -> str:
    """Modifies a query. Currently handles:
    - adds/overwrites the target catalog of tables within a query
    - replaces unsupported function FROM_ISO8601_TIMESTAMP with TO_TIMESTAMP
    """
    def _handle_node(node: Expression, catalog: str) -> Expression:

        # adjust namespace
        if isinstance(node, Table):
            if node.db != "":  # only overwrite catalog when node has schema! tables without schema-identifier are currently not handled. Needed?
                node.set("catalog", Identifier(this=catalog, quoted=node.parts[0].quoted))

        try:
            # handle unsupported FROM_ISO8601_TIMESTAMP
            ts_function = node.find(exp.FromISO8601Timestamp)
            if ts_function:
                ts_function.replace(
                    exp.func(
                        "to_timestamp",
                        *ts_function.args.values()
                    )
                )
        except Exception as ex:
            print(ex)

        return node

    sql = sqlglot.parse_one(
        sql=query.lstrip().rstrip(),
        dialect=dialect,
    ).transform(
        fun=_handle_node,
        catalog=catalog,
    ).sql(
        dialect=dialect,
    )
    return sql
