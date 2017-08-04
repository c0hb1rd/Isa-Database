"""

"""
from enum import Enum


# 数据类型映射
TYPE_MAP = {
    'int': int,
    'float': float,
    'str': str,
    'INT': int,
    'FLOAT': float,
    'VARCHAR': str
}


# 字段类型枚举
class FieldType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT = float = 'float'


# 字段主键枚举
class FieldKey(Enum):
    PRIMARY = 'PRIMARY KEY'
    INCREMENT = 'AUTO_INCREMENT'
    UNIQUE = 'UNIQUE'
    NOT_NULL = 'NOT NULL'
    NULL = 'NULL'
