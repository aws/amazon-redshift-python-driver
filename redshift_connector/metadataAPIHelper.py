import logging
import re
import typing
from typing import Optional, Tuple
from enum import IntEnum

from redshift_connector.error import (
    MISSING_MODULE_ERROR_MSG,
    InterfaceError,
    ProgrammingError,
)
from redshift_connector.utils.oids import RedshiftOID
from redshift_connector.utils.sql_types import SQLType

_logger: logging.Logger = logging.getLogger(__name__)


class ResultSetMetadata:
    def __init__(self, column_count: int, columns: typing.Dict[str, int]):
        self._column_count: int = column_count
        self._columns: typing.Dict[str, int] = columns

    @property
    def column_count(self) -> int:
        return self._column_count

    @property
    def columns(self) -> typing.Dict[str, int]:
        return self._columns


# Enum representing different types of procedures based on their result behavior
class ProcedureType(IntEnum):
    RESULT_UNKNOWN = 0
    NO_RESULT = 1
    RETURNS_RESULT = 2


# Enum representing different types of parameters in a procedure
class ProcedureColumnType(IntEnum):
    UNKNOWN = 0
    IN = 1
    IN_OUT = 2
    OUT = 3
    RETURN = 4
    RESULT = 5


# Enum representing different types of functions based on their return type
class FunctionType(IntEnum):
    UNKNOWN = 0
    NOTABLE = 1
    RETURNS_TABLE = 2


# Enum representing different types of parameters in a function
class FunctionColumnType(IntEnum):
    UNKNOWN = 0
    IN = 1
    IN_OUT = 2
    OUT = 3
    RETURN = 4
    RESULT = 5


class RedshiftDataTypes:
    VALID_TYPES = {
        # Numeric types
        'smallint', 'int2',
        'integer', 'int', 'int4',
        'bigint', 'int8',
        'decimal', 'numeric',
        'real', 'float4',
        'double precision', 'float8', 'float',

        # Character types
        'char', 'character', 'nchar', 'bpchar',
        'varchar', 'character varying', 'nvarchar', 'text',

        # Date/Time types
        'date',
        'time', 'time without time zone',
        'timetz', 'time with time zone',
        'timestamp', 'timestamp without time zone',
        'timestamptz', 'timestamp with time zone',
        'intervaly2m', 'interval year to month',
        'intervald2s', 'interval day to second',

        # Other types
        'boolean', 'bool',
        'hllsketch',
        'super',
        'varbyte', 'varbinary', 'binary varying',
        'geometry',
        'geography',

        # Legacy data types
        'oid',
        'smallint[]',
        'pg_attribute',
        'pg_type',
        'refcursor'
    }

    @classmethod
    def is_valid_type(cls, data_type: str) -> bool:
        """Check if a single data type is valid."""
        return data_type.lower() in cls.VALID_TYPES

    @classmethod
    def validate_types(cls, data_types: typing.List[str]) -> tuple[bool, typing.List[str]]:
        """
        Validate a list of Redshift data types against the predefined valid types.

        Args:
            data_types: List of data type strings to validate (e.g., ['integer', 'varchar', 'timestamp'])

        Returns:
            tuple: A 2-element tuple containing:
                - bool: True if all data types are valid, False if any are invalid
                - List[str]: List of invalid data types found (empty if all are valid)
        """
        invalid_types = [dtype for dtype in data_types
                         if not cls.is_valid_type(dtype)]
        return len(invalid_types) == 0, invalid_types


class MetadataAPIHelper:
    def __init__(self) -> None:
        self._empty_string: str = ""
        self._initialize_numeric_constants()
        self._initialize_column_name_constants()
        self._initialize_result_set_metadata()
        self._initialize_sql_queries()

    def _initialize_numeric_constants(self) -> None:
        self._row_description_col_label_index: int = 0
        self._imported_key_no_action: int = 3
        self._imported_key_not_deferrable: int = 7
        self._procedure_no_nulls: int = 0
        self._procedure_nullable: int = 1
        self._procedure_nullable_unknown: int = 2
        self._function_no_nulls: int = 0
        self._function_nullable: int = 1
        self._function_nullable_unknown: int = 2
        self._get_imported_key_pk_catalog_index: int = 0
        self._get_imported_key_pk_schema_index: int = 1
        self._get_imported_key_pk_table_index: int = 2
        self._get_imported_key_key_seq_index: int = 8
        self._get_exported_key_fk_catalog_index: int = 4
        self._get_exported_key_fk_schema_index: int = 5
        self._get_exported_key_fk_table_index: int = 6
        self._get_exported_key_key_seq_index: int = 8
        self._default_radix: int = 10

    def _initialize_column_name_constants(self) -> None:
        self._SHOW_DATABASES_database_name: str = 'database_name'

        self._SHOW_SCHEMA_database_name: str = 'database_name'
        self._SHOW_SCHEMA_schema_name: str = 'schema_name'

        self._SHOW_TABLES_database_name: str = 'database_name'
        self._SHOW_TABLES_schema_name: str = 'schema_name'
        self._SHOW_TABLES_table_name: str = 'table_name'
        self._SHOW_TABLES_table_type: str = 'table_type'
        self._SHOW_TABLES_remarks: str = 'remarks'
        self._SHOW_TABLES_owner: str = 'owner'
        self._SHOW_TABLES_last_altered_time: str = 'last_altered_time'
        self._SHOW_TABLES_last_modified_time: str = 'last_modified_time'
        self._SHOW_TABLES_dist_style: str = 'dist_style'
        self._SHOW_TABLES_table_subtype: str = 'table_subtype'

        self._SHOW_COLUMNS_database_name: str = "database_name"
        self._SHOW_COLUMNS_schema_name: str = "schema_name"
        self._SHOW_COLUMNS_table_name: str = "table_name"
        self._SHOW_COLUMNS_procedure_name: str = "procedure_name"
        self._SHOW_COLUMNS_function_name: str = "function_name"
        self._SHOW_COLUMNS_column_name: str = "column_name"
        self._SHOW_COLUMNS_column_type: str = "column_type"
        self._SHOW_COLUMNS_ordinal_position: str = "ordinal_position"
        self._SHOW_COLUMNS_column_default: str = "column_default"
        self._SHOW_COLUMNS_is_nullable: str = "is_nullable"
        self._SHOW_COLUMNS_data_type: str = "data_type"
        self._SHOW_COLUMNS_character_maximum_length: str = "character_maximum_length"
        self._SHOW_COLUMNS_numeric_precision: str = "numeric_precision"
        self._SHOW_COLUMNS_numeric_scale: str = "numeric_scale"
        self._SHOW_COLUMNS_remarks: str = "remarks"
        self._SHOW_COLUMNS_specific_name: str = "specific_name"
        self._SHOW_COLUMNS_sort_key_type: str = "sort_key_type"
        self._SHOW_COLUMNS_sort_key: str = "sort_key"
        self._SHOW_COLUMNS_dist_key: str = "dist_key"
        self._SHOW_COLUMNS_encoding: str = "encoding"
        self._SHOW_COLUMNS_collation: str = "collation"

        self._SHOW_CONSTRAINTS_PK_database_name: str = "database_name"
        self._SHOW_CONSTRAINTS_PK_schema_name: str = "schema_name"
        self._SHOW_CONSTRAINTS_PK_table_name: str = "table_name"
        self._SHOW_CONSTRAINTS_PK_column_name: str = "column_name"
        self._SHOW_CONSTRAINTS_PK_key_seq: str = "key_seq"
        self._SHOW_CONSTRAINTS_PK_pk_name: str = "pk_name"

        self._SHOW_CONSTRAINTS_FK_pk_database_name: str = "pk_database_name"
        self._SHOW_CONSTRAINTS_FK_pk_schema_name: str = "pk_schema_name"
        self._SHOW_CONSTRAINTS_FK_pk_table_name: str = "pk_table_name"
        self._SHOW_CONSTRAINTS_FK_pk_column_name: str = "pk_column_name"
        self._SHOW_CONSTRAINTS_FK_fk_database_name: str = "fk_database_name"
        self._SHOW_CONSTRAINTS_FK_fk_schema_name: str = "fk_schema_name"
        self._SHOW_CONSTRAINTS_FK_fk_table_name: str = "fk_table_name"
        self._SHOW_CONSTRAINTS_FK_fk_column_name: str = "fk_column_name"
        self._SHOW_CONSTRAINTS_FK_key_seq: str = "key_seq"
        self._SHOW_CONSTRAINTS_FK_update_rule: str = "update_rule"
        self._SHOW_CONSTRAINTS_FK_delete_rule: str = "delete_rule"
        self._SHOW_CONSTRAINTS_FK_fk_name: str = "fk_name"
        self._SHOW_CONSTRAINTS_FK_pk_name: str = "pk_name"
        self._SHOW_CONSTRAINTS_FK_deferrability: str = "deferrability"

        self._SHOW_GRANT_database_name: str = "database_name"
        self._SHOW_GRANT_schema_name: str = "schema_name"
        self._SHOW_GRANT_object_name: str = "object_name"
        self._SHOW_GRANT_table_name: str = "table_name"
        self._SHOW_GRANT_column_name: str = "column_name"
        self._SHOW_GRANT_grantor_name: str = "grantor_name"
        self._SHOW_GRANT_identity_name: str = "identity_name"
        self._SHOW_GRANT_privilege_type: str = "privilege_type"
        self._SHOW_GRANT_admin_option: str = "admin_option"

        self._SHOW_PROCEDURES_database_name: str = "database_name"
        self._SHOW_PROCEDURES_schema_name: str = "schema_name"
        self._SHOW_PROCEDURES_procedure_name: str = "procedure_name"
        self._SHOW_PROCEDURES_return_type: str = "return_type"
        self._SHOW_PROCEDURES_argument_list: str = "argument_list"

        self._SHOW_FUNCTIONS_database_name: str = "database_name"
        self._SHOW_FUNCTIONS_schema_name: str = "schema_name"
        self._SHOW_FUNCTIONS_function_name: str = "function_name"
        self._SHOW_FUNCTIONS_return_type: str = "return_type"
        self._SHOW_FUNCTIONS_argument_list: str = "argument_list"

        self._SHOW_PARAMETERS_database_name: str = "database_name"
        self._SHOW_PARAMETERS_schema_name: str = "schema_name"
        self._SHOW_PARAMETERS_procedure_name: str = "procedure_name"
        self._SHOW_PARAMETERS_function_name: str = "function_name"
        self._SHOW_PARAMETERS_parameter_name: str = "parameter_name"
        self._SHOW_PARAMETERS_ordinal_position: str = "ordinal_position"
        self._SHOW_PARAMETERS_parameter_type: str = "parameter_type"
        self._SHOW_PARAMETERS_data_type: str = "data_type"
        self._SHOW_PARAMETERS_character_maximum_length: str = "character_maximum_length"
        self._SHOW_PARAMETERS_numeric_precision: str = "numeric_precision"
        self._SHOW_PARAMETERS_numeric_scale: str = "numeric_scale"

        self._specific_name: str = "specific_name"

    def _initialize_result_set_metadata(self) -> None:
        self._get_catalogs_result_metadata = ResultSetMetadata(
            column_count = 1,
            columns = {"TABLE_CAT": int(RedshiftOID.VARCHAR)}
        )

        self._get_schemas_result_metadata = ResultSetMetadata(

            column_count = 2,
            columns = {"TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "TABLE_CATALOG": int(RedshiftOID.VARCHAR)}
        )

        self._get_tables_result_metadata = ResultSetMetadata(
            column_count = 15,
            columns = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
                       "TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "TABLE_NAME": int(RedshiftOID.VARCHAR),
                       "TABLE_TYPE": int(RedshiftOID.VARCHAR),
                       "REMARKS": int(RedshiftOID.VARCHAR),
                       "TYPE_CAT": int(RedshiftOID.VARCHAR),
                       "TYPE_SCHEM": int(RedshiftOID.VARCHAR),
                       "TYPE_NAME": int(RedshiftOID.VARCHAR),
                       "SELF_REFERENCING_COL_NAME": int(RedshiftOID.VARCHAR),
                       "REF_GENERATION": int(RedshiftOID.VARCHAR),
                       "OWNER": int(RedshiftOID.VARCHAR),
                       "LAST_ALTERED_TIME": int(RedshiftOID.TIMESTAMP),
                       "LAST_MODIFIED_TIME": int(RedshiftOID.TIMESTAMP),
                       "DIST_STYLE": int(RedshiftOID.VARCHAR),
                       "TABLE_SUBTYPE": int(RedshiftOID.VARCHAR)}
        )

        self._get_columns_result_metadata = ResultSetMetadata(
            column_count = 29,
            columns = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
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
                       "IS_GENERATEDCOLUMN": int(RedshiftOID.VARCHAR),
                       "SORT_KEY_TYPE": int(RedshiftOID.VARCHAR),
                       "SORT_KEY": int(RedshiftOID.INTEGER),
                       "DIST_KEY": int(RedshiftOID.INTEGER),
                       "ENCODING": int(RedshiftOID.VARCHAR),
                       "COLLATION": int(RedshiftOID.VARCHAR)}
        )

        self._get_primary_keys_result_metadata = ResultSetMetadata(

            column_count = 6,
            columns = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
                       "TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "TABLE_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "KEY_SEQ": int(RedshiftOID.SMALLINT),
                       "PK_NAME": int(RedshiftOID.VARCHAR)}
        )

        self._get_foreign_keys_result_metadata = ResultSetMetadata(
            column_count = 14,
            columns = {"PKTABLE_CAT": int(RedshiftOID.VARCHAR),
                       "PKTABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "PKTABLE_NAME": int(RedshiftOID.VARCHAR),
                       "PKCOLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "FKTABLE_CAT": int(RedshiftOID.VARCHAR),
                       "FKTABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "FKTABLE_NAME": int(RedshiftOID.VARCHAR),
                       "FKCOLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "KEY_SEQ": int(RedshiftOID.SMALLINT),
                       "UPDATE_RULE": int(RedshiftOID.SMALLINT),
                       "DELETE_RULE": int(RedshiftOID.SMALLINT),
                       "FK_NAME": int(RedshiftOID.VARCHAR),
                       "PK_NAME": int(RedshiftOID.VARCHAR),
                       "DEFERRABILITY": int(RedshiftOID.SMALLINT)}
        )

        self._get_best_row_identifier_result_metadata = ResultSetMetadata(
            column_count=8,
            columns = {"SCOPE": int(RedshiftOID.SMALLINT),
                       "COLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "DATA_TYPE": int(RedshiftOID.INTEGER),
                       "TYPE_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_SIZE": int(RedshiftOID.INTEGER),
                       "BUFFER_LENGTH": int(RedshiftOID.INTEGER),
                       "DECIMAL_DIGITS": int(RedshiftOID.SMALLINT),
                       "PSEUDO_COLUMN": int(RedshiftOID.SMALLINT)}
        )

        self._get_column_privileges_result_metadata = ResultSetMetadata(
            column_count=8,
            columns = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
                       "TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "TABLE_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "GRANTOR": int(RedshiftOID.VARCHAR),
                       "GRANTEE": int(RedshiftOID.VARCHAR),
                       "PRIVILEGE": int(RedshiftOID.VARCHAR),
                       "IS_GRANTABLE": int(RedshiftOID.VARCHAR)}
        )

        self._get_table_privileges_result_metadata = ResultSetMetadata(
            column_count=7,
            columns = {"TABLE_CAT": int(RedshiftOID.VARCHAR),
                       "TABLE_SCHEM": int(RedshiftOID.VARCHAR),
                       "TABLE_NAME": int(RedshiftOID.VARCHAR),
                       "GRANTOR": int(RedshiftOID.VARCHAR),
                       "GRANTEE": int(RedshiftOID.VARCHAR),
                       "PRIVILEGE": int(RedshiftOID.VARCHAR),
                       "IS_GRANTABLE": int(RedshiftOID.VARCHAR)}
        )

        self._get_procedures_result_metadata = ResultSetMetadata(
            column_count=9,
            columns = {"PROCEDURE_CAT": int(RedshiftOID.VARCHAR),
                       "PROCEDURE_SCHEM": int(RedshiftOID.VARCHAR),
                       "PROCEDURE_NAME": int(RedshiftOID.VARCHAR),
                       "RESERVE1": int(RedshiftOID.VARCHAR),
                       "RESERVE2": int(RedshiftOID.VARCHAR),
                       "RESERVE3": int(RedshiftOID.VARCHAR),
                       "REMARKS": int(RedshiftOID.VARCHAR),
                       "PROCEDURE_TYPE": int(RedshiftOID.SMALLINT),
                       "SPECIFIC_NAME": int(RedshiftOID.VARCHAR)}
        )

        self._get_procedure_columns_result_metadata = ResultSetMetadata(
            column_count=20,
            columns = {"PROCEDURE_CAT": int(RedshiftOID.VARCHAR),
                       "PROCEDURE_SCHEM": int(RedshiftOID.VARCHAR),
                       "PROCEDURE_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_TYPE": int(RedshiftOID.SMALLINT),
                       "DATA_TYPE": int(RedshiftOID.INTEGER),
                       "TYPE_NAME": int(RedshiftOID.VARCHAR),
                       "PRECISION": int(RedshiftOID.INTEGER),
                       "LENGTH": int(RedshiftOID.INTEGER),
                       "SCALE": int(RedshiftOID.SMALLINT),
                       "RADIX": int(RedshiftOID.SMALLINT),
                       "NULLABLE": int(RedshiftOID.SMALLINT),
                       "REMARKS": int(RedshiftOID.VARCHAR),
                       "COLUMN_DEF": int(RedshiftOID.VARCHAR),
                       "SQL_DATA_TYPE": int(RedshiftOID.INTEGER),
                       "SQL_DATETIME_SUB": int(RedshiftOID.INTEGER),
                       "CHAR_OCTET_LENGTH": int(RedshiftOID.INTEGER),
                       "ORDINAL_POSITION": int(RedshiftOID.INTEGER),
                       "IS_NULLABLE": int(RedshiftOID.VARCHAR),
                       "SPECIFIC_NAME": int(RedshiftOID.VARCHAR)}
        )

        self._get_functions_result_metadata = ResultSetMetadata(
            column_count=6,
            columns = {"FUNCTION_CAT": int(RedshiftOID.VARCHAR),
                       "FUNCTION_SCHEM": int(RedshiftOID.VARCHAR),
                       "FUNCTION_NAME": int(RedshiftOID.VARCHAR),
                       "REMARKS": int(RedshiftOID.VARCHAR),
                       "FUNCTION_TYPE": int(RedshiftOID.SMALLINT),
                       "SPECIFIC_NAME": int(RedshiftOID.VARCHAR)}
        )

        self._get_function_columns_result_metadata = ResultSetMetadata(
            column_count=17,
            columns = {"FUNCTION_CAT": int(RedshiftOID.VARCHAR),
                       "FUNCTION_SCHEM": int(RedshiftOID.VARCHAR),
                       "FUNCTION_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_NAME": int(RedshiftOID.VARCHAR),
                       "COLUMN_TYPE": int(RedshiftOID.SMALLINT),
                       "DATA_TYPE": int(RedshiftOID.INTEGER),
                       "TYPE_NAME": int(RedshiftOID.VARCHAR),
                       "PRECISION": int(RedshiftOID.INTEGER),
                       "LENGTH": int(RedshiftOID.INTEGER),
                       "SCALE": int(RedshiftOID.SMALLINT),
                       "RADIX": int(RedshiftOID.SMALLINT),
                       "NULLABLE": int(RedshiftOID.SMALLINT),
                       "REMARKS": int(RedshiftOID.VARCHAR),
                       "CHAR_OCTET_LENGTH": int(RedshiftOID.INTEGER),
                       "ORDINAL_POSITION": int(RedshiftOID.INTEGER),
                       "IS_NULLABLE": int(RedshiftOID.VARCHAR),
                       "SPECIFIC_NAME": int(RedshiftOID.VARCHAR)}
        )

        self._get_table_type_result_metadata = ResultSetMetadata(
            column_count=1,
            columns = {"TABLE_TYPE": int(RedshiftOID.VARCHAR)}
        )

    def _initialize_sql_queries(self) -> None:
        # define SQL for SHOW command
        self._sql_show_databases: str = "SHOW DATABASES;"
        self._sql_show_schemas: str = "SHOW SCHEMAS FROM DATABASE %s;"
        self._sql_show_schemas_like: str = "SHOW SCHEMAS FROM DATABASE %s LIKE %s;"
        self._sql_show_tables: str = "SHOW TABLES FROM SCHEMA %s.%s;"
        self._sql_show_tables_like: str = "SHOW TABLES FROM SCHEMA %s.%s LIKE %s;"
        self._sql_show_columns: str = "SHOW COLUMNS FROM TABLE %s.%s.%s;"
        self._sql_show_columns_like: str = "SHOW COLUMNS FROM TABLE %s.%s.%s LIKE %s;"
        self._sql_show_constraints_pk: str = "SHOW CONSTRAINTS PRIMARY KEYS FROM TABLE %s.%s.%s;"
        self._sql_show_constraints_fk: str = "SHOW CONSTRAINTS FOREIGN KEYS FROM TABLE %s.%s.%s;"
        self._sql_show_constraints_fk_ex: str = "SHOW CONSTRAINTS FOREIGN KEYS EXPORTED FROM TABLE %s.%s.%s;"
        self._sql_show_grant_column: str = "SHOW COLUMN GRANTS ON TABLE %s.%s.%s;"
        self._sql_show_grant_column_like: str = "SHOW COLUMN GRANTS ON TABLE %s.%s.%s LIKE %s;"
        self._sql_show_grant_table: str = "SHOW GRANTS ON TABLE %s.%s.%s;"
        self._sql_show_procedures: str = "SHOW PROCEDURES FROM SCHEMA %s.%s;"
        self._sql_show_procedures_like: str = "SHOW PROCEDURES FROM SCHEMA %s.%s LIKE %s;"
        self._sql_show_functions: str = "SHOW FUNCTIONS FROM SCHEMA %s.%s;"
        self._sql_show_functions_like: str = "SHOW FUNCTIONS FROM SCHEMA %s.%s LIKE %s;"
        self._sql_show_parameters_procedure: str = "SHOW PARAMETERS OF PROCEDURE %s.%s.%s"
        self._sql_show_parameters_function: str = "SHOW PARAMETERS OF FUNCTION %s.%s.%s"
        self._sql_semicolon = ";"
        self._sql_like: str = " LIKE %s;"

    # Mapping of string parameter types to ProcedureColumnType enum values
    __procedure_column_type_map = {
        'IN': ProcedureColumnType.IN,
        'INOUT': ProcedureColumnType.IN_OUT,
        'OUT': ProcedureColumnType.OUT,
        'TABLE': ProcedureColumnType.RESULT,
        'RETURN': ProcedureColumnType.RETURN
    }

    # Mapping of string parameter types to FunctionColumnType enum values
    __function_column_type_map = {
        'IN': FunctionColumnType.IN,
        'INOUT': FunctionColumnType.IN_OUT,
        'OUT': FunctionColumnType.OUT,
        'TABLE': FunctionColumnType.RESULT,
        'RETURN': FunctionColumnType.RETURN
    }

    __rs_type_map = {
        "character varying": "varchar",
        '"char"': "char",
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
        "binary varying": "varbyte",
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
        "varbyte": int(SQLType.SQL_LONGVARBINARY),
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
        "intervald2s": 64,
    }

    __buffer_length = {
        "bool": 1,
        "bit": 1,
        "boolean": 1,
        "int2": 2,
        "smallint": 2,
        "int4": 4,
        "integer": 4,
        "int": 4,
        "int8": 20,
        "bigint": 20,
        "float4": 4,
        "real": 4,
        "float8": 8,
        "double precision": 8,
        "date": 6,
        "time": 15,
        "timetz": 21,
        "timestamp": 6,
        "timestamptz": 35,
        "intervaly2m": 4,
        "intervald2s": 8,
        "super": 4194304,
        "geography": 1000000,
        "varbyte": 1000000
    }

    def get_second_fraction(self, data_type: Optional[str] = None) -> Tuple[str, bool, int]:
        date_time_customize_precision: bool = False
        precisions: int = 6
        if data_type and (
            re.match(r"(time|timetz|timestamp|timestamptz).*\(\d+\)", data_type)
            or re.match(r"interval.*\(\d+\)", data_type)
        ):
            rs_type = self.get_rs_type(str(re.sub(r"\(\d+\)", "", data_type).rstrip()))
            match = re.search(r"\(\d+\)", data_type)
            precisions = int(match.group(0)[1:-1]) if match else 6
            date_time_customize_precision = True
        else:
            rs_type = self.get_rs_type(data_type or "")

        return rs_type, date_time_customize_precision, precisions

    def get_rs_type(self, rs_type: str) -> str:
        return self.__rs_type_map.get(rs_type, rs_type)

    def get_sql_type(self, rs_type: str) -> int:
        return self.__sql_type_mapping.get(rs_type, int(SQLType.SQL_OTHER))

    def get_column_size(self, rs_type: str, numeric_precision: int, character_maximum_length: int) -> Optional[int]:
        if rs_type == "numeric" or rs_type == "decimal":
            return int(numeric_precision)
        elif (
            rs_type == "varchar"
            or rs_type == "character varying"
            or rs_type == "char"
            or rs_type == "character"
            or rs_type == "bpchar"
        ):
            return int(character_maximum_length)
        elif rs_type == "super" or rs_type == "geometry" or rs_type == "geography" or rs_type == "varbyte":
            return None
        else:
            return self.__data_type_length.get(rs_type, 2147483647)

    def get_column_length(self, rs_type: str) -> typing.Optional[int]:
        return self.__buffer_length.get(rs_type, None)

    @staticmethod
    def get_decimal_digits(rs_type: str, numeric_scale: int, precision: int, customizePrecision: bool) -> Optional[int]:
        if rs_type == "float4" or rs_type == "real":
            return 8
        elif rs_type == "float8" or rs_type == "double precision":
            return 17
        elif rs_type == "numeric" or rs_type == "decimal":
            return int(numeric_scale)
        elif (
            rs_type == "time"
            or rs_type == "timetz"
            or rs_type == "timestamp"
            or rs_type == "timestamptz"
            or rs_type == "intervald2s"
        ):
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
    def get_auto_increment(col_def: Optional[str]) -> str:
        if col_def is not None and ('"identity"' in col_def or "default_identity" in col_def):
            return "YES"
        else:
            return "NO"

    @staticmethod
    def is_none_or_empty(input_str: Optional[str]) -> bool:
        return input_str is None or input_str == ""

    @staticmethod
    def get_specific_name(name: str, argument_list: str) -> str:
        """
        Generates a specific name for a procedure or function by combining its name and argument list.

        Args:
            name (str): The name of the procedure or function. Must not be None or empty.
            argument_list (str): A string representation of the argument list. Can be None or empty.

        Returns:
            str: A string in the format "name(argument_list)". If argument_list is None or empty,
                 it will return "name()".

        Raises:
            ProgrammingError: If the provided name is None or an empty string.

        """
        if MetadataAPIHelper.is_none_or_empty(name):
            raise ProgrammingError("Procedure/Function name should not be null or empty")
        argument_list = "" if argument_list is None else argument_list
        return name + "(" + argument_list + ")"

    def create_parameterized_query_string(self, argument_list: str, sql_base: str, column_name_pattern: str) -> typing.Tuple[str, list[str]]:
        """
        Creates a parameterized SQL query string based on the argument list and base SQL statement.
        Appends either a semicolon or LIKE clause depending on whether column_name_pattern is provided.

        Args:
            argument_list (str): Comma-separated string of argument types
                               (e.g. "integer, short, character varying")
            sql_base (str): Base SQL statement to which parameters will be added
                           (e.g. "SHOW PARAMETERS OF PROCEDURE")
            column_name_pattern (str): Optional pattern for filtering column names.
                                     If provided, adds LIKE clause instead of semicolon

        Returns:
            tuple[str, list[str]]: A tuple containing:
            - str: Complete SQL query with appropriate placeholders and termination
                  (e.g. "SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s, %s, %s);" or
                        "SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s, %s, %s) LIKE %s;")
            - list[str]: List of argument types with whitespace stripped
                  (e.g. ["integer", "short", "character varying"])

        Examples:
            create_parameterized_query_string("integer, short, varchar", "SHOW PARAMS", None)
            -> ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s, %s, %s);", ["integer", "short", "varchar"])

            create_parameterized_query_string("integer", "SHOW PARAMS", "pattern%")
            -> ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s(%s) LIKE %s;", ["integer"])

            create_parameterized_query_string("", "SHOW PARAMS", None)
            -> ("SHOW PARAMETERS OF PROCEDURE %s.%s.%s();", [])
        """
        if not argument_list:
            sql = f"{sql_base}()" + (self._sql_semicolon if self.is_none_or_empty(column_name_pattern) else self._sql_like)
            return sql, []

        # Split the argument list
        args = [arg for arg in argument_list.split(', ')]

        # Check for empty arguments before validation
        if '' in args:
            raise ValueError(f"Empty argument found in argument list: {argument_list}. All arguments must be valid data types.")

        # Validate arguments
        if len(args) != 0:
            is_valid, invalid_types = RedshiftDataTypes.validate_types(args)
            if not is_valid:
                raise ValueError(
                    f"Invalid data type(s) in argument list: {', '.join(invalid_types)}. "
                    f"Argument list provided: {argument_list}"
                )

        # Create the parameterized string
        placeholders = ", ".join(["%s"] * len(args))
        sql = f"{sql_base}({placeholders})" + (self._sql_semicolon if self.is_none_or_empty(column_name_pattern) else self._sql_like)
        return sql, args

    @staticmethod
    def get_procedure_type(return_type: str) -> int:
        """
        Determines the procedure type based on return type
        Args:
            return_type: The return type of the procedure
        Returns:
            ProcedureType enum value indicating if procedure returns result
        """
        if MetadataAPIHelper.is_none_or_empty(return_type):
            return ProcedureType.NO_RESULT
        return ProcedureType.RETURNS_RESULT

    @staticmethod
    def get_function_type(return_type: str) -> int:
        """
        Determines the function type based on return type
        Args:
            return_type: The return type of the function
        Returns:
            FunctionType enum value indicating if function returns table
        """
        if return_type is not None and return_type == "record":
            return FunctionType.RETURNS_TABLE
        return FunctionType.NOTABLE

    def get_procedure_column_type(self, parameter_type: str) -> int:
        """
        Maps string parameter type to ProcedureColumnType enum value
        Args:
            parameter_type: String representation of parameter type
        Returns:
            Corresponding ProcedureColumnType enum value
        """
        if parameter_type is None:
            return ProcedureColumnType.UNKNOWN
        return self.__procedure_column_type_map.get(parameter_type.upper(), ProcedureColumnType.UNKNOWN)

    def get_function_column_type(self, parameter_type: str) -> int:
        """
        Maps string parameter type to FunctionColumnType enum value
        Args:
            parameter_type: String representation of parameter type
        Returns:
            Corresponding FunctionColumnType enum value
        """
        if parameter_type is None:
            return FunctionColumnType.UNKNOWN
        return self.__function_column_type_map.get(parameter_type.upper(), FunctionColumnType.UNKNOWN)

    @staticmethod
    def get_is_grantable(admin_option: bool) -> typing.Optional[str]:
        """This function bridges the gap where SHOW return boolean but JDBC spec required YES/NO"""
        if admin_option is not None:
            return "YES" if admin_option else "NO"
        return None

    @staticmethod
    def pattern_match(s: str, pattern: str) -> bool:
        """
        Pattern matching function similar to SQL LIKE operator. Driver currently treat empty string as null
        which match anything
        Args:
            s: input string to match against the pattern
            pattern: pattern string containing wildcards
                    '%' matches zero or more characters
                    '_' matches exactly one character
        Returns:
            bool: True if string matches pattern, False otherwise
        """
        # Empty pattern matches any string
        if not pattern:
            return True

        # Convert SQL LIKE pattern to regex
        regex_pattern = MetadataAPIHelper.convert_sql_like_to_regex(pattern)

        # Use regex to match
        return bool(re.match(regex_pattern, s, re.DOTALL))

    @staticmethod
    def convert_sql_like_to_regex(pattern: str) -> str:
        """
        Convert SQL LIKE pattern to regex pattern
        Args:
            pattern: SQL LIKE pattern with % and _ wildcards
        Returns:
            str: Equivalent regex pattern
        """
        # Check if pattern only contains '%'
        if all(c == '%' for c in pattern):
            return r'.*'

        regex_pattern = ""
        i = 0

        while i < len(pattern):
            char = pattern[i]

            if char == '\\' and i + 1 < len(pattern):
                # Handle escaped characters
                next_char = pattern[i + 1]
                if next_char in ['%', '_', '\\']:
                    # Escape the next character for regex
                    regex_pattern += re.escape(next_char)
                    i += 2
                else:
                    # Not a special escape, treat backslash literally
                    regex_pattern += re.escape(char)
                    i += 1
            elif char == '%':
                # % matches zero or more characters
                regex_pattern += '.*'
                i += 1
            elif char == '_':
                # _ matches exactly one character
                regex_pattern += '.'
                i += 1
            else:
                # Regular character, escape it for regex
                regex_pattern += re.escape(char)
                i += 1

        return f'^{regex_pattern}$'
