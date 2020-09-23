from collections import defaultdict
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple, Union

from ..aioctxlib import aiocontextmanager
from ..patterns import var_pattern
from ..types import Parameters, SQLOperationType


class MaybeAcquire:
    def __init__(self, client: Any) -> None:
        self.client = client

    async def __aenter__(self) -> Any:
        if "acquire" in dir(self.client):
            self._managed_conn = await self.client.acquire()
            return self._managed_conn
        else:
            self._managed_conn = None
            return self.client

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._managed_conn is not None:
            await self.client.release(self._managed_conn)


class AsyncPGAdapter:
    is_aio_driver = True

    def __init__(self) -> None:
        self.var_replacements: Dict[str, Dict] = defaultdict(dict)

    def process_sql(self, query_name: str, _op_type: SQLOperationType, sql: str) -> str:
        count = 0
        adj = 0

        for match in var_pattern.finditer(sql):
            gd = match.groupdict()
            # Do nothing if the match is found within quotes.
            if gd["dblquote"] is not None or gd["quote"] is not None:
                continue

            var_name = gd["var_name"]
            if var_name in self.var_replacements[query_name]:
                replacement = f"${self.var_replacements[query_name][var_name]}"
            else:
                count += 1
                replacement = f"${count}"
                self.var_replacements[query_name][var_name] = count

            start = match.start() + len(gd["lead"]) + adj
            end = match.end() - len(gd["trail"]) + adj

            sql = sql[:start] + replacement + sql[end:]

            replacement_len = len(replacement)
            # the lead ":" char is the reason for the +1
            var_len = len(var_name) + 1
            if replacement_len < var_len:
                adj = adj + replacement_len - var_len
            else:
                adj = adj + var_len - replacement_len

        return sql

    def maybe_order_params(
        self, query_name: str, parameters: Union[Dict, Tuple]
    ) -> Union[Tuple, List]:
        if isinstance(parameters, dict):
            xs = [(self.var_replacements[query_name][k], v) for k, v in parameters.items()]
            xs = sorted(xs, key=lambda x: x[0])
            return [x[1] for x in xs]
        elif isinstance(parameters, tuple):
            return parameters
        else:
            raise ValueError(f"Parameters expected to be dict or tuple, received {parameters}")

    async def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable] = None,
    ) -> Any:
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(conn) as connection:
            results = await connection.fetch(sql, *parameters)
            if record_class is not None:
                results = [record_class(**dict(rec)) for rec in results]
        return results

    async def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable] = None,
    ) -> Optional[Any]:
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(conn) as connection:
            result = await connection.fetchrow(sql, *parameters)
            if result is not None and record_class is not None:
                result = record_class(**dict(result))
        return result

    async def select_value(self, conn, query_name, sql, parameters):
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(conn) as connection:
            return await connection.fetchval(sql, *parameters)

    @aiocontextmanager
    async def select_cursor(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> AsyncGenerator[Any, Any]:
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(conn) as connection:
            stmt = await connection.prepare(sql)
            async with connection.transaction():
                yield stmt.cursor(*parameters)

    async def insert_returning(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> Any:
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(conn) as connection:
            res = await connection.fetchrow(sql, *parameters)
            if res:
                return res[0] if len(res) == 1 else res
            else:
                return None

    async def insert_update_delete(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> None:
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(conn) as connection:
            await connection.execute(sql, *parameters)

    async def insert_update_delete_many(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> None:
        parameters = [self.maybe_order_params(query_name, params) for params in parameters]
        async with MaybeAcquire(conn) as connection:
            await connection.executemany(sql, parameters)

    @staticmethod
    async def execute_script(conn: Any, sql: str) -> None:
        async with MaybeAcquire(conn) as connection:
            return await connection.execute(sql)
