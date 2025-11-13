from typing import Any

from pathlib import Path

import yaml
from pydantic import StrictStr, StrictInt, StrictBool
from pydantic_settings import BaseSettings


class SourceOptions(BaseSettings):
    """Settings-Class for validating source related options"""
    dialect: StrictStr = "trino"
    path: StrictStr


class TargetOptions(BaseSettings):
    """Settings-Class for validating target related options"""
    dialect: StrictStr = "databricks"
    host: StrictStr
    warehouse_id: StrictStr
    catalog: StrictStr
    token: StrictStr
    http_path: StrictStr
    connection_url: StrictStr

    def __init__(self, **config: Any) -> None:
        super().__init__(
            **config,
            http_path=f"/sql/1.0/warehouses/{config['warehouse_id']}",
            connection_url=f"databricks://token:{config['token']}@{config['host']}"
                           f"?http_path=/sql/1.0/warehouses/{config['warehouse_id']}"
        )


class ResultOptions(BaseSettings):
    """Settings-Class for validating result related options"""
    catalog: StrictStr


class Config(BaseSettings):
    """Settings-Class for validating cli-configurations & convenience."""

    name: StrictStr
    workload: StrictInt
    persist_queries: StrictBool
    validation: StrictBool
    out_path: StrictStr
    query_runs: StrictInt
    parallelism: StrictInt
    source: SourceOptions
    target: TargetOptions
    results: ResultOptions

    def __init__(self, **config: Any) -> None:
        file = config.pop("file")

        with Path(file).open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)

        super().__init__(**data,)
