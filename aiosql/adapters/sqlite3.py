from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

from ..types import Parameters, SQLOperationType


class SQLite3DriverAdapter:
    @staticmethod
    def process_sql(_query_name: str, _op_type: SQLOperationType, sql: str) -> str:
        """Pass through function because the ``sqlite3`` driver already handles the :var_name
        "named style" syntax used by aiosql variables. Note, it will also accept "qmark style"
        variables.

        Args:
            _query_name (str): The name of the sql query. Unused.
            _op_type (aiosql.SQLOperationType): The type of SQL operation performed by the sql.
            sql (str): The sql as written before processing.

        Returns:
            str: Original SQL text unchanged.
        """
        return sql

    @staticmethod
    def select(
        conn: Any,
        _query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable] = None,
    ) -> Any:
        cur = conn.cursor()
        cur.execute(sql, parameters)
        results = cur.fetchall()
        if record_class is not None:
            column_names = [c[0] for c in cur.description]
            results = [record_class(**dict(zip(column_names, row))) for row in results]
        cur.close()
        return results

    @staticmethod
    def select_one(
        conn: Any,
        _query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable] = None,
    ) -> Optional[Any]:
        cur = conn.cursor()
        cur.execute(sql, parameters)
        result = cur.fetchone()
        if result is not None and record_class is not None:
            column_names = [c[0] for c in cur.description]
            result = record_class(**dict(zip(column_names, result)))
        cur.close()
        return result

    @staticmethod
    def select_value(conn, _query_name, sql, parameters):
        cur = conn.cursor()
        cur.execute(sql, parameters)
        result = cur.fetchone()
        cur.close()
        return result[0] if result else None

    @staticmethod
    @contextmanager
    def select_cursor(
        conn: Any, _query_name: str, sql: str, parameters: Parameters
    ) -> Iterator[Any]:
        cur = conn.cursor()
        cur.execute(sql, parameters)
        try:
            yield cur
        finally:
            cur.close()

    @staticmethod
    def insert_update_delete(conn: Any, _query_name: str, sql: str, parameters: Parameters) -> None:
        conn.execute(sql, parameters)

    @staticmethod
    def insert_update_delete_many(
        conn: Any, _query_name: str, sql: str, parameters: Parameters
    ) -> None:
        conn.executemany(sql, parameters)

    @staticmethod
    def insert_returning(conn: Any, _query_name: str, sql: str, parameters: Parameters) -> Any:
        cur = conn.cursor()
        cur.execute(sql, parameters)
        results = cur.lastrowid
        cur.close()
        return results

    @staticmethod
    def execute_script(conn: Any, sql: str) -> None:
        conn.executescript(sql)
        return "DONE"
