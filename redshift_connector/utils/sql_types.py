from enum import EnumMeta, IntEnum


class SQLTypeMeta(EnumMeta):
    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise AttributeError(f"Cannot modify SQL type constant '{name}'")
            # throw error if any constant value defined in SQLType was modified
            # e.g. "Cannot modify SQL type constant 'SQL_VARCHAR'"
        super().__setattr__(name, value)


class SQLType(IntEnum, metaclass=SQLTypeMeta):
    SQL_VARCHAR = 12
    SQL_BIT = -7
    SQL_TINYINT = -6
    SQL_SMALLINT = 5
    SQL_INTEGER = 4
    SQL_BIGINT = -5
    SQL_FLOAT = 6
    SQL_REAL = 7
    SQL_DOUBLE = 8
    SQL_NUMERIC = 2
    SQL_DECIMAL = 3
    SQL_CHAR = 1
    SQL_LONGVARCHAR = -1
    SQL_DATE = 91
    SQL_TIME = 92
    SQL_TIMESTAMP = 93
    SQL_BINARY = -2
    SQL_VARBINARY = -3
    SQL_LONGVARBINARY = -4
    SQL_NULL = 0
    SQL_OTHER = 1111
    SQL_BOOLEAN = 16
    SQL_LONGNVARCHAR = -16
    SQL_TIME_WITH_TIMEZONE = 2013
    SQL_TIMESTAMP_WITH_TIMEZONE = 2014


def get_sql_type_name(sql_type: int) -> str:
    return SQLType(sql_type).name
