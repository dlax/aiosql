from typing import Any, AsyncGenerator, Callable, Optional

from ..aioctxlib import aiocontextmanager
from ..types import Parameters, SQLOperationType


class AioSQLiteAdapter:
    is_aio_driver = True

    @staticmethod
    def process_sql(_query_name: str, _op_type: SQLOperationType, sql: str) -> str:
        """Pass through function because the ``aiosqlite`` driver can already handle the
        :var_name format used by aiosql and doesn't need any additional processing.

        Args:
            _query_name (str): The name of the sql query.
            _op_type (SQLOperationType): The type of SQL operation performed by the query.
            sql (str): The sql as written before processing.

        Returns:
            str: Original SQL text unchanged.
        """
        return sql

    @staticmethod
    async def select(
        conn: Any,
        _query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable] = None,
    ) -> Any:
        async with conn.execute(sql, parameters) as cur:
            results = await cur.fetchall()
            if record_class is not None:
                column_names = [c[0] for c in cur.description]
                results = [record_class(**dict(zip(column_names, row))) for row in results]
        return results

    @staticmethod
    async def select_one(
        conn: Any,
        _query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable] = None,
    ) -> Optional[Any]:
        async with conn.execute(sql, parameters) as cur:
            result = await cur.fetchone()
            if result is not None and record_class is not None:
                column_names = [c[0] for c in cur.description]
                result = record_class(**dict(zip(column_names, result)))
        return result

    @staticmethod
    async def select_value(conn, _query_name, sql, parameters):
        async with conn.execute(sql, parameters) as cur:
            result = await cur.fetchone()
        return result[0] if result else None

    @staticmethod
    @aiocontextmanager
    async def select_cursor(
        conn: Any, _query_name: str, sql: str, parameters: Parameters
    ) -> AsyncGenerator[Any, Any]:
        async with conn.execute(sql, parameters) as cur:
            yield cur

    @staticmethod
    async def insert_returning(
        conn: Any, _query_name: str, sql: str, parameters: Parameters
    ) -> Any:
        async with conn.execute(sql, parameters) as cur:
            return cur.lastrowid

    @staticmethod
    async def insert_update_delete(
        conn: Any, _query_name: str, sql: str, parameters: Parameters
    ) -> None:
        cur = await conn.execute(sql, parameters)
        await cur.close()

    @staticmethod
    async def insert_update_delete_many(
        conn: Any, _query_name: str, sql: str, parameters: Parameters
    ) -> None:
        cur = await conn.executemany(sql, parameters)
        await cur.close()

    @staticmethod
    async def execute_script(conn: Any, sql: str) -> None:
        await conn.executescript(sql)
        return "DONE"
