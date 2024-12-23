import logging
import typing
import re


from redshift_connector.utils.oids import RedshiftOID
from redshift_connector.utils.sql_types import SQLType

from redshift_connector.error import (
    MISSING_MODULE_ERROR_MSG,
    InterfaceError,
    ProgrammingError,
)

_logger: logging.Logger = logging.getLogger(__name__)


class MetadataAPIHelper:
    _row_description_col_label_index: int = 0

    _CatalogsColNum: int = 1
    _get_catalogs_col: typing.Dict = {"TABLE_CAT": int(RedshiftOID.VARCHAR)}

    _SchemasColNum: int = 2
    _get_schemas_col: typing.Dict = {"TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                                          "TABLE_CATALOG": int(RedshiftOID.VARCHAR)}

    _TablesColNum: int = 10
    _get_tables_col: typing.Dict = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
                                         "TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                                         "TABLE_NAME": int(RedshiftOID.VARCHAR),
                                         "TABLE_TYPE": int(RedshiftOID.VARCHAR),
                                         "REMARKS": int(RedshiftOID.VARCHAR),
                                         "TYPE_CAT": int(RedshiftOID.VARCHAR),
                                         "TYPE_SCHEM": int(RedshiftOID.VARCHAR),
                                         "TYPE_NAME": int(RedshiftOID.VARCHAR),
                                         "SELF_REFERENCING_COL_NAME": int(RedshiftOID.VARCHAR),
                                         "REF_GENERATION": int(RedshiftOID.VARCHAR)}

    _ColumnsColNum: int = 24
    _get_columns_col: typing.Dict = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
                                          "TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                                          "TABLE_NAME": int(RedshiftOID.VARCHAR),
                                          "COLUMN_NAME": int(RedshiftOID.VARCHAR),
                                          "DATA_TYPE": int(RedshiftOID.INTEGER),
                                          "TYPE_NAME": int(RedshiftOID.VARCHAR),
                                          "COLUMN_SIZE": int(RedshiftOID.INTEGER),
                                          "BUFFER_LENGTH": int(RedshiftOID.INTEGER),
                                          "DECIMAL_DIGITS": int(RedshiftOID.INTEGER),
                                          "NUM_PREC_RADIX": int(RedshiftOID.INTEGER),
                                          "NULLABLE": int(RedshiftOID.INTEGER),
                                          "REMARKS": int(RedshiftOID.VARCHAR),
                                          "COLUMN_DEF": int(RedshiftOID.VARCHAR),
                                          "SQL_DATA_TYPE": int(RedshiftOID.INTEGER),
                                          "SQL_DATETIME_SUB": int(RedshiftOID.INTEGER),
                                          "CHAR_OCTET_LENGTH": int(RedshiftOID.INTEGER),
                                          "ORDINAL_POSITION": int(RedshiftOID.INTEGER),
                                          "IS_NULLABLE": int(RedshiftOID.VARCHAR),
                                          "SCOPE_CATALOG": int(RedshiftOID.VARCHAR),
                                          "SCOPE_SCHEMA": int(RedshiftOID.VARCHAR),
                                          "SCOPE_TABLE": int(RedshiftOID.VARCHAR),
                                          "SOURCE_DATA_TYPE": int(RedshiftOID.SMALLINT),
                                          "IS_AUTOINCREMENT": int(RedshiftOID.VARCHAR),
                                          "IS_GENERATEDCOLUMN": int(RedshiftOID.VARCHAR)}

    _SHOW_DATABASES_database_name: str = 'database_name'

    _SHOW_SCHEMA_database_name: str = 'database_name'
    _SHOW_SCHEMA_schema_name: str = 'schema_name'

    _SHOW_TABLES_database_name: str = 'database_name'
    _SHOW_TABLES_schema_name: str = 'schema_name'
    _SHOW_TABLES_table_name: str = 'table_name'
    _SHOW_TABLES_table_type: str = 'table_type'
    _SHOW_TABLES_remarks: str = 'remarks'

    _SHOW_COLUMNS_database_name: str = "database_name"
    _SHOW_COLUMNS_schema_name: str = "schema_name"
    _SHOW_COLUMNS_table_name: str = "table_name"
    _SHOW_COLUMNS_column_name: str = "column_name"
    _SHOW_COLUMNS_ordinal_position: str = "ordinal_position"
    _SHOW_COLUMNS_column_default: str = "column_default"
    _SHOW_COLUMNS_is_nullable: str = "is_nullable"
    _SHOW_COLUMNS_data_type: str = "data_type"
    _SHOW_COLUMNS_character_maximum_length: str = "character_maximum_length"
    _SHOW_COLUMNS_numeric_precision: str = "numeric_precision"
    _SHOW_COLUMNS_numeric_scale: str = "numeric_scale"
    _SHOW_COLUMNS_remarks: str = "remarks"

    _sql_show_databases: str = "SHOW DATABASES;"
    _sql_show_databases_like: str = "SHOW DATABASES LIKE {0};"
    _sql_show_schemas: str = "SHOW SCHEMAS FROM DATABASE {0};"
    _sql_show_schemas_like: str = "SHOW SCHEMAS FROM DATABASE {0} LIKE {1};"
    _sql_show_tables: str = "SHOW TABLES FROM SCHEMA {0}.{1};"
    _sql_show_tables_like: str = "SHOW TABLES FROM SCHEMA {0}.{1} LIKE {2};"
    _sql_show_columns: str = "SHOW COLUMNS FROM TABLE {0}.{1}.{2};"
    _sql_show_columns_like: str = "SHOW COLUMNS FROM TABLE {0}.{1}.{2} LIKE {3};"

    # define constant for QUOTE_IDENT()
    _prepare_quote_ident: str = "select pg_catalog.QUOTE_IDENT(%s); "
    _quote_iden_result_row: int = 0
    _quote_iden_result_col: int = 0

    # define constant for QUOTE_LITERAL
    _prepare_quote_literal: str = "select pg_catalog.QUOTE_LITERAL(%s); "
    _quote_literal_result_row: int = 0
    _quote_literal_result_col: int = 0

    __rs_type_map = {
        "character varying": "varchar",
        "\"char\"": "char",
        "character": "char",
        "smallint": "int2",
        "integer": "int4",
        "bigint": "int8",
        "real": "float4",
        "double precision": "float8",
        "boolean": "bool",
        "time without time zone": "time",
        "time with time zone": "timetz",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
        "interval year to month": "intervaly2m",
        "interval year": "intervaly2m",
        "interval month": "intervaly2m",
        "interval day to second": "intervald2s",
        "interval day": "intervald2s",
        "interval second": "intervald2s",
        "binary varying": "varbyte"
    }

    __sql_type_mapping = {
        "varchar": int(SQLType.SQL_VARCHAR),
        "char": int(SQLType.SQL_CHAR),
        "int2": int(SQLType.SQL_SMALLINT),
        "int4": int(SQLType.SQL_INTEGER),
        "int8": int(SQLType.SQL_BIGINT),
        "float4": int(SQLType.SQL_REAL),
        "float8": int(SQLType.SQL_DOUBLE),
        "numeric": int(SQLType.SQL_NUMERIC),
        "bool": int(SQLType.SQL_BIT),
        "date": int(SQLType.SQL_DATE),
        "time": int(SQLType.SQL_TIME),
        "timetz": int(SQLType.SQL_TIME_WITH_TIMEZONE),
        "timestamp": int(SQLType.SQL_TIMESTAMP),
        "timestamptz": int(SQLType.SQL_TIMESTAMP_WITH_TIMEZONE),
        "intervaly2m": int(SQLType.SQL_OTHER),
        "intervald2s": int(SQLType.SQL_OTHER),
        "super": int(SQLType.SQL_LONGVARCHAR),
        "geometry": int(SQLType.SQL_LONGVARBINARY),
        "geography": int(SQLType.SQL_LONGVARBINARY),
        "varbyte": int(SQLType.SQL_LONGVARBINARY)
    }

    __data_type_length = {
        "bool": 1,
        "bit": 1,
        "boolean": 1,
        "int2": 5,
        "smallint": 5,
        "int4": 10,
        "integer": 10,
        "int": 10,
        "int8": 19,
        "bigint": 19,
        "float4": 8,
        "real": 8,
        "float8": 17,
        "double precision": 17,
        "date": 13,
        "time": 15,
        "timetz": 21,
        "timestamp": 29,
        "timestamptz": 35,
        "intervaly2m": 32,
        "intervald2s": 64
    }

    def get_second_fraction(self, data_type: str = None) -> (str, bool):
        date_time_customize_precision: bool = False
        precisions: int = 6
        if re.match(r"(time|timetz|timestamp|timestamptz).*\(\d+\)", data_type) or re.match(r"interval.*\(\d+\)", data_type):
            rs_type = self.get_rs_type(str(re.sub(r"\(\d+\)", "", data_type).rstrip()))
            precisions  = int(re.search(r"\(\d+\)", data_type).group(0)[1:-1])
            date_time_customize_precision = True
        else:
            rs_type = self.get_rs_type(data_type)

        return rs_type, date_time_customize_precision, precisions

    def get_rs_type(self, rs_type: str) -> str:
        return self.__rs_type_map.get(rs_type, rs_type)

    def get_sql_type(self,rs_type: str) -> int:
        return self.__sql_type_mapping.get(rs_type, int(SQLType.SQL_OTHER))


    def get_column_size(self, rs_type: str, numeric_precision: int, character_maximum_length: int) -> int:
        if rs_type == "numeric" or rs_type == "decimal":
            return int(numeric_precision)
        elif rs_type == "varchar" or rs_type == "character varying" or rs_type == "char" or rs_type == "character" or rs_type == "bpchar":
            return int(character_maximum_length)
        elif rs_type == "super" or rs_type == "geometry" or rs_type == "geography" or rs_type == "varbyte":
            return None
        else:
            return self.__data_type_length.get(rs_type, 2147483647)

    @staticmethod
    def get_decimal_digits(rs_type: str, numeric_scale: int, precision: int, customizePrecision: bool) -> int:
        if rs_type == "float4" or rs_type == "real":
            return 8
        elif rs_type == "float8" or rs_type == "double precision":
            return 17
        elif rs_type == "numeric" or rs_type == "decimal":
            return int(numeric_scale)
        elif rs_type == "time" or rs_type == "timetz" or rs_type == "timestamp" or rs_type == "timestamptz" or rs_type == "intervald2s":
            return precision if customizePrecision else 6
        elif rs_type == "super" or rs_type == "geometry" or rs_type == "geography" or rs_type == "varbyte":
            return None
        else:
            return 0

    @staticmethod
    def get_num_prefix_radix(rs_type: str) -> int:
        if rs_type == "geography" or rs_type == "varbyte":
            return 2
        else:
            return 10

    @staticmethod
    def get_nullable(nullable: str) -> int:
        if nullable == "YES":
            return 1
        elif nullable == "NO":
            return 0
        else:
            return 2

    @staticmethod
    def get_auto_increment(col_def: str) -> str:
        if col_def is not None and ("\"identity\"" in col_def or "default_identity" in col_def):
            return "YES"
        else:
            return "NO"

    @staticmethod
    def is_none_or_empty(input_str: str) -> bool:
        return input_str is None or input_str == ""
