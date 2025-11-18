import typing
import pytest  # type: ignore
import redshift_connector
from dataclasses import dataclass
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, pattern_matches, exact_matches, run_metadata_test, SKIP_RESULT_VALIDATION

# Define constants
is_single_database_metadata_case: typing.List[bool] = [True, False]
current_catalog: str = "test_catalog"


# Define column constant for get_catalogs
get_catalogs_col_name: typing.List[str] = [
    "TABLE_CAT"
]


# Define result structure for get_catalogs
@dataclass
class CatalogInfo:
    table_cat: str


# Define expected returned result for get_catalogs
get_catalogs_single_result: typing.List[CatalogInfo] = [
    CatalogInfo(current_catalog)
]

get_catalogs_multiple_result: typing.List[CatalogInfo] = [
    CatalogInfo("awsdatacatalog"),
    CatalogInfo("dev"),
    CatalogInfo(current_catalog)
]


# Define column constant for get_schemas
get_schemas_col_name: typing.List[str] = [
    "TABLE_SCHEM",
    "TABLE_CATALOG"
]


# Define result structure for get_schemas
@dataclass
class SchemaInfo:
    table_schem: str
    table_catalog: str


# Define expected returned result for get_schemas
get_schemas_result: typing.List[SchemaInfo] = [
    SchemaInfo("test_schema_1",current_catalog),
    SchemaInfo("test_schema_2",current_catalog)
]


# Define column constant for get_tables
get_tables_col_name: typing.List[str] = [
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "TABLE_TYPE",
    "REMARKS",
    "TYPE_CAT",
    "TYPE_SCHEM",
    "TYPE_NAME",
    "SELF_REFERENCING_COL_NAME",
    "REF_GENERATION",
    "OWNER",
    "LAST_ALTERED_TIME",
    "LAST_MODIFIED_TIME",
    "DIST_STYLE",
    "TABLE_SUBTYPE"
]


# Define result structure for get_tables
@dataclass
class TableInfo:
    table_cat: str
    table_schem: str
    table_name: str
    table_type: str
    remarks: str
    type_cat: str
    type_schem: str
    type_name: str
    self_referencing_col_name: str
    ref_generation: str


# Define expected returned result for get_tables
# skip validation for remarks due to known issue in catalog
get_tables_result: typing.List[TableInfo] = [
    TableInfo(current_catalog, "test_schema_1", "test_table_1", "TABLE", SKIP_RESULT_VALIDATION, "", "", "", "", ""),
    TableInfo(current_catalog, "test_schema_1", "test_table_2", "TABLE", SKIP_RESULT_VALIDATION, "", "", "", "", ""),
    TableInfo(current_catalog, "test_schema_2", "test_table_3", "TABLE", SKIP_RESULT_VALIDATION, "", "", "", "", ""),
    TableInfo(current_catalog, "test_schema_2", "test_table_4", "TABLE", SKIP_RESULT_VALIDATION, "", "", "", "", "")
]


# Define column constant for get_columns
get_columns_col_name: typing.List[str] = [
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "COLUMN_NAME",
    "DATA_TYPE",
    "TYPE_NAME",
    "COLUMN_SIZE",
    "BUFFER_LENGTH",
    "DECIMAL_DIGITS",
    "NUM_PREC_RADIX",
    "NULLABLE",
    "REMARKS",
    "COLUMN_DEF",
    "SQL_DATA_TYPE",
    "SQL_DATETIME_SUB",
    "CHAR_OCTET_LENGTH",
    "ORDINAL_POSITION",
    "IS_NULLABLE",
    "SCOPE_CATALOG",
    "SCOPE_SCHEMA",
    "SCOPE_TABLE",
    "SOURCE_DATA_TYPE",
    "IS_AUTOINCREMENT",
    "IS_GENERATEDCOLUMN",
    "SORT_KEY_TYPE",
    "SORT_KEY",
    "DIST_KEY",
    "ENCODING",
    "COLLATION"
]


# Define result structure for get_columns
@dataclass
class ColumnInfo:
    table_cat: str
    table_schem: str
    table_name: str
    column_name: str
    data_type: int
    type_name: str
    column_size: int
    buffer_length: typing.Optional[int]
    decimal_digits: int
    num_prec_radix: int
    nullable: int
    remarks: typing.Optional[str]
    column_def: typing.Optional[str]
    sql_data_type: int
    sql_datetime_sub: typing.Optional[int]
    char_octet_length: typing.Optional[int]
    ordinal_position: int
    is_nullable: str
    scope_catalog: typing.Optional[str]
    scope_schema: typing.Optional[str]
    scope_table: typing.Optional[str]
    source_data_type: typing.Optional[int]
    is_autoincrement: str
    is_generatedcolumn: str


# Define expected returned result for get_columns
# skip validation for remarks / column def due to known issue in catalog
get_columns_result: typing.List[ColumnInfo] = [
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_smallint', 5, 'int2', 5, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 5, None, 5, 1, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_integer', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 2, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_integer_auto', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 3, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_bigint', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 4, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_bigint_auto', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 5, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_numeric', 2, 'numeric', 10, None, 5, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2, None, 10, 6, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_real', 7, 'float4', 8, None, 8, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 7, None, 8, 7, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_double', 8, 'float8', 17, None, 17, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 8, None, 17, 8, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_boolean', -7, 'bool', 1, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -7, None, 1, 9, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_char', 1, 'char', 20, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1, None, 20, 10, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_varchar', 12, 'varchar', 256, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 12, None, 256, 11, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_date', 91, 'date', 13, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 91, None, 13, 12, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_time', 92, 'time', 15, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 92, None, 15, 13, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_timetz', 2013, 'timetz', 21, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2013, None, 21, 14, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_timestamp', 93, 'timestamp', 29, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 93, None, 29, 15, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_timestamptz', 2014, 'timestamptz', 35, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2014, None, 35, 16, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_intervaly2m', 1111, 'intervaly2m', 32, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 32, 17, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_intervald2s', 1111, 'intervald2s', 64, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 18, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_1", 'test_column_intervald2s_cus', 1111, 'intervald2s', 64, None, 4, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 19, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_smallint', 5, 'int2', 5, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 5, None, 5, 1, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_integer', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 2, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_integer_auto', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 3, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_bigint', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 4, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_bigint_auto', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 5, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_numeric', 2, 'numeric', 10, None, 5, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2, None, 10, 6, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_real', 7, 'float4', 8, None, 8, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 7, None, 8, 7, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_double', 8, 'float8', 17, None, 17, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 8, None, 17, 8, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_boolean', -7, 'bool', 1, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -7, None, 1, 9, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_char', 1, 'char', 20, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1, None, 20, 10, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_varchar', 12, 'varchar', 256, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 12, None, 256, 11, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_date', 91, 'date', 13, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 91, None, 13, 12, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_time', 92, 'time', 15, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 92, None, 15, 13, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_timetz', 2013, 'timetz', 21, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2013, None, 21, 14, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_timestamp', 93, 'timestamp', 29, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 93, None, 29, 15, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_timestamptz', 2014, 'timestamptz', 35, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2014, None, 35, 16, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_intervaly2m', 1111, 'intervaly2m', 32, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 32, 17, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_intervald2s', 1111, 'intervald2s', 64, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 18, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_1", "test_table_2", 'test_column_intervald2s_cus', 1111, 'intervald2s', 64, None, 4, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 19, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_smallint', 5, 'int2', 5, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 5, None, 5, 1, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_integer', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 2, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_integer_auto', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 3, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_bigint', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 4, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_bigint_auto', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 5, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_numeric', 2, 'numeric', 10, None, 5, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2, None, 10, 6, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_real', 7, 'float4', 8, None, 8, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 7, None, 8, 7, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_double', 8, 'float8', 17, None, 17, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 8, None, 17, 8, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_boolean', -7, 'bool', 1, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -7, None, 1, 9, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_char', 1, 'char', 20, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1, None, 20, 10, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_varchar', 12, 'varchar', 256, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 12, None, 256, 11, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_date', 91, 'date', 13, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 91, None, 13, 12, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_time', 92, 'time', 15, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 92, None, 15, 13, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_timetz', 2013, 'timetz', 21, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2013, None, 21, 14, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_timestamp', 93, 'timestamp', 29, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 93, None, 29, 15, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_timestamptz', 2014, 'timestamptz', 35, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2014, None, 35, 16, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_intervaly2m', 1111, 'intervaly2m', 32, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 32, 17, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_intervald2s', 1111, 'intervald2s', 64, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 18, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_3", 'test_column_intervald2s_cus', 1111, 'intervald2s', 64, None, 4, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 19, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_smallint', 5, 'int2', 5, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 5, None, 5, 1, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_integer', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 2, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_integer_auto', 4, 'int4', 10, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 4, None, 10, 3, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_bigint', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 4, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_bigint_auto', -5, 'int8', 19, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -5, None, 19, 5, 'YES', None, None, None, None, 'YES', 'YES'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_numeric', 2, 'numeric', 10, None, 5, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2, None, 10, 6, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_real', 7, 'float4', 8, None, 8, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 7, None, 8, 7, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_double', 8, 'float8', 17, None, 17, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 8, None, 17, 8, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_boolean', -7, 'bool', 1, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, -7, None, 1, 9, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_char', 1, 'char', 20, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1, None, 20, 10, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_varchar', 12, 'varchar', 256, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 12, None, 256, 11, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_date', 91, 'date', 13, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 91, None, 13, 12, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_time', 92, 'time', 15, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 92, None, 15, 13, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_timetz', 2013, 'timetz', 21, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2013, None, 21, 14, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_timestamp', 93, 'timestamp', 29, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 93, None, 29, 15, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_timestamptz', 2014, 'timestamptz', 35, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 2014, None, 35, 16, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_intervaly2m', 1111, 'intervaly2m', 32, None, 0, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 32, 17, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_intervald2s', 1111, 'intervald2s', 64, None, 6, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 18, 'YES', None, None, None, None, 'NO', 'NO'),
    ColumnInfo(current_catalog, "test_schema_2", "test_table_4", 'test_column_intervald2s_cus', 1111, 'intervald2s', 64, None, 4, 10, 1, SKIP_RESULT_VALIDATION, SKIP_RESULT_VALIDATION, 1111, None, 64, 19, 'YES', None, None, None, None, 'NO', 'NO')
]


# Define column constant for get_table_types
get_table_types_col_name: typing.List[str] = [
    "TABLE_TYPE"
]


# Define result structure for get_table_types
@dataclass
class TableTypeInfo:
    table_type: str


# Define expected returned result for get_table_types
get_table_types_result: typing.List[TableTypeInfo] = [
    TableTypeInfo("EXTERNAL TABLE"),
    TableTypeInfo("EXTERNAL VIEW"),
    TableTypeInfo("LOCAL TEMPORARY"),
    TableTypeInfo("TABLE"),
    TableTypeInfo("VIEW")
]


startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS {} cascade;".format("test_schema_1"),
    "DROP SCHEMA IF EXISTS {} cascade;".format("test_schema_2"),
    "CREATE SCHEMA {}".format("test_schema_1"),
    "CREATE SCHEMA {}".format("test_schema_2"),
    "CREATE TABLE test_schema_1.test_table_1(test_column_smallint smallint, test_column_integer integer, test_column_integer_auto integer IDENTITY(1,1), test_column_bigint bigint, test_column_bigint_auto bigint IDENTITY(1,1), test_column_numeric numeric(10,5), test_column_real real, test_column_double double precision, test_column_boolean boolean, test_column_char char(20), test_column_varchar varchar(256), test_column_date date, test_column_time time, test_column_timetz timetz, test_column_timestamp timestamp, test_column_timestamptz timestamptz, test_column_intervaly2m interval year to month, test_column_intervald2s interval day to second, test_column_intervald2s_cus interval day to second (4));",
    "CREATE TABLE test_schema_1.test_table_2(test_column_smallint smallint, test_column_integer integer, test_column_integer_auto integer IDENTITY(1,1), test_column_bigint bigint, test_column_bigint_auto bigint IDENTITY(1,1), test_column_numeric numeric(10,5), test_column_real real, test_column_double double precision, test_column_boolean boolean, test_column_char char(20), test_column_varchar varchar(256), test_column_date date, test_column_time time, test_column_timetz timetz, test_column_timestamp timestamp, test_column_timestamptz timestamptz, test_column_intervaly2m interval year to month, test_column_intervald2s interval day to second, test_column_intervald2s_cus interval day to second (4));",
    "CREATE TABLE test_schema_2.test_table_3(test_column_smallint smallint, test_column_integer integer, test_column_integer_auto integer IDENTITY(1,1), test_column_bigint bigint, test_column_bigint_auto bigint IDENTITY(1,1), test_column_numeric numeric(10,5), test_column_real real, test_column_double double precision, test_column_boolean boolean, test_column_char char(20), test_column_varchar varchar(256), test_column_date date, test_column_time time, test_column_timetz timetz, test_column_timestamp timestamp, test_column_timestamptz timestamptz, test_column_intervaly2m interval year to month, test_column_intervald2s interval day to second, test_column_intervald2s_cus interval day to second (4));",
    "CREATE TABLE test_schema_2.test_table_4(test_column_smallint smallint, test_column_integer integer, test_column_integer_auto integer IDENTITY(1,1), test_column_bigint bigint, test_column_bigint_auto bigint IDENTITY(1,1), test_column_numeric numeric(10,5), test_column_real real, test_column_double double precision, test_column_boolean boolean, test_column_char char(20), test_column_varchar varchar(256), test_column_date date, test_column_time time, test_column_timetz timetz, test_column_timestamp timestamp, test_column_timestamptz timestamptz, test_column_intervaly2m interval year to month, test_column_intervald2s interval day to second, test_column_intervald2s_cus interval day to second (4));"
)


catalogs_case = [None, "%", "", current_catalog, current_catalog + "%", "wrong_database"]
schemas_case = ["test_schema_1", "test_schema_%", "wrong_schema"]
tables_case = [None, "%", "", "test_table_1", "test%", "wrong_table"]
columns_case = [None, "%", "", "test_column_smallint", "test_column_%", "wrong_column"]
tables_types_case = [[], ["TABLE", "VIEW"], ["TAB"]]


class TestMetadataAPI:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_config(self, request, db_kwargs):
        setup_metadata_test_env(db_kwargs, current_catalog, startup_stmts)

        def fin():
            teardown_metadata_test_env(db_kwargs, current_catalog)
        request.addfinalizer(fin)

    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_catalogs(self, db_kwargs, is_single_database_metadata) -> None:
        expected_result: typing.List[CatalogInfo] = get_catalogs_single_result if is_single_database_metadata else get_catalogs_multiple_result

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=2,
            method_name="get_catalogs",
            method_args=(),
            expected_col_name=get_catalogs_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_schemas(self, db_kwargs, catalog, schema, is_single_database_metadata) -> None:
        expected_result: typing.List[SchemaInfo] = self.get_schemas_matches(catalog, schema)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=2,
            method_name='get_schemas',
            method_args=(catalog, schema),
            expected_col_name=get_schemas_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("table", tables_case)
    @pytest.mark.parametrize("table_type", tables_types_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_tables(self, db_kwargs, catalog, schema, table, table_type, is_single_database_metadata) -> None:
        expected_result: typing.List[TableInfo] = self.get_tables_matches(catalog, schema, table, table_type)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=2,
            method_name='get_tables',
            method_args=(catalog, schema, table, table_type),
            expected_col_name=get_tables_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("table", tables_case)
    @pytest.mark.parametrize("column", columns_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_columns(self, db_kwargs, catalog, schema, table, column, is_single_database_metadata) -> None:
        expected_result: typing.List[ColumnInfo] = self.get_columns_matches(catalog, schema, table, column)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=2,
            method_name='get_columns',
            method_args=(catalog, schema, table, column),
            expected_col_name=get_columns_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_table_type(self, db_kwargs, is_single_database_metadata) -> None:
        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=2,
            method_name='get_table_types',
            method_args=(),
            expected_col_name=get_table_types_col_name,
            expected_result=get_table_types_result
        )

    @staticmethod
    def get_schemas_matches(catalog, schema) -> typing.List[SchemaInfo]:
        result: typing.List[SchemaInfo] = []
        for schema_info in get_schemas_result:
            if exact_matches(catalog, schema_info.table_catalog) and pattern_matches(schema, schema_info.table_schem):
                result.append(schema_info)
        return result

    @staticmethod
    def get_tables_matches(catalog, schema, table, table_type) -> typing.List[TableInfo]:
        result: typing.List[TableInfo] = []
        for table_info in get_tables_result:
            if ((exact_matches(catalog, table_info.table_cat)
                and pattern_matches(schema, table_info.table_schem)
                and pattern_matches(table, table_info.table_name))
                    and (table_type is None or len(table_type) == 0 or table_info.table_type in table_type)):
                result.append(table_info)
        return result

    @staticmethod
    def get_columns_matches(catalog, schema, table, column) -> typing.List[ColumnInfo]:
        result: typing.List[ColumnInfo] = []
        for column_info in get_columns_result:
            if (exact_matches(catalog, column_info.table_cat)
                    and pattern_matches(schema, column_info.table_schem)
                    and pattern_matches(table, column_info.table_name)
                    and pattern_matches(column, column_info.column_name)):
                result.append(column_info)
        return result
