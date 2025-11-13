import math
import multiprocessing
from typing import Callable, Any

import polars as pl
import multiprocessing as mp
from functools import partial


def unnest_with_prefix(df: pl.DataFrame, column: str, prefix: str, separator: str = "_") -> pl.DataFrame:
    """Unnests a struct column and adds a prefix to unnested columns."""
    if not any([col for col in df.columns if col == column and df[col].dtype == pl.Struct]):
        raise RuntimeError(f"column {column} not found in dataframe or is not of type {type(pl.Struct)}")

    return df.with_columns(
        [
            pl.col(column).struct.rename_fields(
                [f"{prefix}{separator}{field_name}" for field_name in df[column].struct.fields]
            )
        ]
    ).unnest(column)


def transform_df(df: pl.DataFrame, func: Callable, parallelism: int = 1, **kwargs: Any) -> pl.DataFrame:
    """Applies a function to a DF with parallelism. Parallelism is applied by dividing the DF
    into chunks where each chunk is processed in parallel.
    """

    if parallelism == 1:
        return func(df=df, **kwargs)

    parallelism = min((parallelism, multiprocessing.cpu_count()))

    chunksize = math.ceil(df.shape[0] / parallelism)
    chunks = list(df.iter_slices(n_rows=chunksize))

    with mp.Pool(processes=parallelism) as pool:
        results = pool.map(partial(func, **kwargs), chunks)

    return pl.concat(results)
