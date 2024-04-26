from enum import Enum


class DBOperator(Enum):
    INSERT = 0  # 新增
    UPDATE = 1  # 更新
    DELETE = 2  # 删除
    UNABLE_PARSED = 7  # 无法解析
    COMPLETING = 8     # 完成但未更新标记
    COMPLETE = 9       # 完成


class RestRet(Enum):
    SUCCEED = 200
    FAILED = 0

