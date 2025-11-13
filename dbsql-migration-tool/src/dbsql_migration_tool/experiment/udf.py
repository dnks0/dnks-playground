import logging
from uuid import UUID
from databricks.sql.client import Connection


logger = logging.getLogger(name=__name__)


def _experiment_udf(query_id: str, query: str, connection: Connection, runs: int) -> list[dict[str, [str | bool]]]:
    """Runs a query against the configured connection. Performs multiple runs if configured."""
    result = []
    for run in range(1, runs + 1):
        with connection.cursor() as cursor:
            try:
                execution = cursor.execute(query)
                statement_id = execution.query_id
                query_result = cursor.fetchone()

            except Exception as ex:
                statement_id = str(UUID(bytes=cursor.active_op_handle.operationId.guid))
                success = False
                msg = str(ex)
                logger.error(
                    "could not run query %s with statement-id %s: %s",
                    query_id,
                    statement_id,
                    str(ex)
                )
            else:
                success = True
                msg = "success"

                if query_result is None:
                    success = False
                    msg = "empty result"

        result.append({"run": run, "statement_id": statement_id, "success": success, "msg": msg})
    return result
