from enum import Enum


class DBOperator(Enum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2
    COMPLETE = 9


class RestRet(Enum):
    SUCCEED = 200
    FAILED = 0

