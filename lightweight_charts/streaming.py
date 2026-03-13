from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

import pandas as pd


def _infer_epoch_unit_from_scalar(value: float) -> str:
    abs_value = abs(float(value))
    if abs_value >= 1e17:
        return 'ns'
    if abs_value >= 1e14:
        return 'us'
    if abs_value >= 1e11:
        return 'ms'
    return 's'


def _normalize_time_value(value):
    if pd.isna(value):
        return value
    if isinstance(value, (int, float)):
        unit = _infer_epoch_unit_from_scalar(value)
        divisor = {'s': 1.0, 'ms': 1000.0, 'us': 1_000_000.0, 'ns': 1_000_000_000.0}[unit]
        return float(value) / divisor
    if isinstance(value, str):
        numeric_value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
        if not pd.isna(numeric_value):
            unit = _infer_epoch_unit_from_scalar(float(numeric_value))
            divisor = {'s': 1.0, 'ms': 1000.0, 'us': 1_000_000.0, 'ns': 1_000_000_000.0}[unit]
            return float(numeric_value) / divisor
    return pd.to_datetime(value).timestamp()


class StreamingSource(ABC):
    @abstractmethod
    def get_latest(self, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_before(self, time_value: float, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_after(self, time_value: float, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    def close(self):
        return None


class PandasSource(StreamingSource):
    def __init__(self, df: pd.DataFrame, time_col: str = 'time'):
        if df is None:
            raise ValueError('df cannot be None')
        if time_col not in df.columns and 'date' in df.columns:
            time_col = 'date'
        if time_col not in df.columns:
            raise NameError(f'No column named "{time_col}" in pandas source.')

        self.time_col = time_col
        self.df = df.copy()
        self.df[self.time_col] = self.df[self.time_col].map(_normalize_time_value)
        self.df = self.df.sort_values(self.time_col).drop_duplicates(subset=[self.time_col], keep='last').reset_index(drop=True)

    def _rename_time(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        if self.time_col == 'time':
            return df.copy()
        return df.rename(columns={self.time_col: 'time'})

    def get_latest(self, limit: int) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame(columns=self.df.columns)
        return self._rename_time(self.df.iloc[-limit:].copy())

    def get_before(self, time_value: float, limit: int) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame(columns=self.df.columns)
        subset = self.df[self.df[self.time_col] < time_value]
        return self._rename_time(subset.iloc[-limit:].copy())

    def get_after(self, time_value: float, limit: int) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame(columns=self.df.columns)
        subset = self.df[self.df[self.time_col] > time_value]
        return self._rename_time(subset.iloc[:limit].copy())


class DuckDBSource(StreamingSource):
    def __init__(
        self,
        database: str,
        table: str,
        time_col: str = 'time',
        time_unit: str = 'timestamp',
        where: Optional[str] = None,
    ):
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                'duckdb is required for DuckDBSource. Install with: pip install duckdb'
            ) from exc

        self._duckdb = duckdb
        self.con = duckdb.connect(database=database, read_only=True)
        self.table = table
        self.time_col = time_col
        self.where = where

        if time_unit not in ('timestamp', 's', 'ms', 'us', 'ns'):
            raise ValueError('time_unit must be one of: timestamp, s, ms, us, ns')
        self.time_unit = time_unit

        info = self.con.execute(f"PRAGMA table_info('{table}')").fetchall()
        self._columns = [row[1] for row in info]
        if self.time_col not in self._columns:
            raise NameError(f'No column named "{self.time_col}" in duckdb table "{table}".')

        self._select_cols = ', '.join(
            [f'{self._quote(c)} AS time' if c == self.time_col and c != 'time' else self._quote(c) for c in self._columns]
        )
        self._time_expr = self._time_expression()

    @staticmethod
    def _quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _time_expression(self) -> str:
        if self.time_unit == 'timestamp':
            return f'epoch({self._quote(self.time_col)})'
        if self.time_unit == 's':
            return f'CAST({self._quote(self.time_col)} AS DOUBLE)'
        if self.time_unit == 'ms':
            return f'CAST({self._quote(self.time_col)} AS DOUBLE) / 1000.0'
        if self.time_unit == 'us':
            return f'CAST({self._quote(self.time_col)} AS DOUBLE) / 1000000.0'
        return f'CAST({self._quote(self.time_col)} AS DOUBLE) / 1000000000.0'

    def _where_clause(self) -> str:
        return f' WHERE {self.where} ' if self.where else ''

    def get_latest(self, limit: int) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame(columns=self._columns)
        query = f'''
            SELECT {self._select_cols}
            FROM {self._quote(self.table)}
            {self._where_clause()}
            ORDER BY {self._quote(self.time_col)} DESC
            LIMIT ?
        '''
        df = self.con.execute(query, [limit]).df()
        return df.sort_values('time').reset_index(drop=True)

    def get_before(self, time_value: float, limit: int) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame(columns=self._columns)
        where_prefix = self._where_clause()
        where_connector = ' AND ' if self.where else ' WHERE '
        query = f'''
            SELECT {self._select_cols}
            FROM {self._quote(self.table)}
            {where_prefix}
            {where_connector} {self._time_expr} < ?
            ORDER BY {self._quote(self.time_col)} DESC
            LIMIT ?
        '''
        df = self.con.execute(query, [time_value, limit]).df()
        return df.sort_values('time').reset_index(drop=True)

    def get_after(self, time_value: float, limit: int) -> pd.DataFrame:
        if limit <= 0:
            return pd.DataFrame(columns=self._columns)
        where_prefix = self._where_clause()
        where_connector = ' AND ' if self.where else ' WHERE '
        query = f'''
            SELECT {self._select_cols}
            FROM {self._quote(self.table)}
            {where_prefix}
            {where_connector} {self._time_expr} > ?
            ORDER BY {self._quote(self.time_col)} ASC
            LIMIT ?
        '''
        return self.con.execute(query, [time_value, limit]).df()

    def close(self):
        self.con.close()


@dataclass
class StreamConfig:
    initial_bars: int = 2000
    chunk_bars: int = 1200
    prefetch_bars: int = 300
    max_bars: int = 20000
    debounce_ms: int = 80
    keep_drawings: bool = True
