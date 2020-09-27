from enum import Enum
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    ContextManager,
    Dict,
    List,
    NamedTuple,
    Optional,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import Protocol


class SQLOperationType(Enum):
    """Enumeration of aiosql operation types.
    """

    INSERT_RETURNING = 0
    INSERT_UPDATE_DELETE = 1
    INSERT_UPDATE_DELETE_MANY = 2
    SCRIPT = 3
    SELECT = 4
    SELECT_ONE = 5
    SELECT_VALUE = 6


class QueryDatum(NamedTuple):
    query_name: str
    doc_comments: str
    operation_type: SQLOperationType
    sql: str
    record_class: Any = None


class QueryFn(Protocol):
    __name__: str
    sql: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


# Can't make this a recursive type in terms of itself
# QueryDataTree = Dict[str, Union[QueryDatum, 'QueryDataTree']]
QueryDataTree = Dict[str, Union[QueryDatum, Dict]]

Parameters = Union[List[Any], Dict[str, Any]]

T = TypeVar("T")


class SyncDriverAdapterProtocol(Protocol):
    def process_sql(self, query_name: str, op_type: SQLOperationType, sql: str) -> str:
        ...

    @overload
    def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: None,
    ) -> List[Any]:
        ...

    @overload
    def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Callable[..., T],
    ) -> List[T]:
        ...

    def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable],
    ) -> Optional[Any]:
        ...

    def select_value(
        self, conn: Any, query_name: str, sql: str, parameters: Union[List, Dict],
    ) -> Optional[Any]:
        ...

    def select_cursor(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> ContextManager[Any]:
        ...

    # TODO: Next major version introduce a return? Optional return?
    def insert_update_delete(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> None:
        ...

    # TODO: Next major version introduce a return? Optional return?
    def insert_update_delete_many(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> None:
        ...

    def insert_returning(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> Optional[Any]:
        ...

    def execute_script(self, conn: Any, sql: str) -> str:
        ...


class AsyncDriverAdapterProtocol(Protocol):
    def process_sql(self, query_name: str, op_type: SQLOperationType, sql: str) -> str:
        ...

    async def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable],
    ) -> List:
        ...

    async def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Parameters,
        record_class: Optional[Callable],
    ) -> Optional[Any]:
        ...

    async def select_value(
        self, conn: Any, query_name: str, sql: str, parameters: Union[List, Dict],
    ) -> Optional[Any]:
        ...

    async def select_cursor(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> AsyncContextManager[Any]:
        ...

    # TODO: Next major version introduce a return? Optional return?
    async def insert_update_delete(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> None:
        ...

    # TODO: Next major version introduce a return? Optional return?
    async def insert_update_delete_many(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> None:
        ...

    async def insert_returning(
        self, conn: Any, query_name: str, sql: str, parameters: Parameters
    ) -> Optional[Any]:
        ...

    async def execute_script(self, conn: Any, sql: str) -> str:
        ...


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol, AsyncDriverAdapterProtocol]
