import typing
import pytest  # type: ignore
import redshift_connector
from dataclasses import dataclass
from enum import IntEnum
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, pattern_matches, exact_matches, run_metadata_test, FunctionType, FunctionColumnType


# Define constants
is_single_database_metadata_case: typing.List[bool] = [True, False]
current_catalog: str = "test_functions_catalog"
test_schema_1: str = "test_functions_schema_1"
test_schema_2: str = "test_functions_schema_2"
test_func_1: str = "test_functions_1_with_input_param"
test_func_2: str = "test_functions_2_empty_input_param"
test_func_3: str = "test_functions_3_empty_input_param"
test_func_4: str = "test_functions_4_empty_input_param"
test_func_1_specific_name: str = "test_functions_1_with_input_param(smallint, integer, bigint, numeric, real, double precision, boolean, character, character varying, date, timestamp without time zone)"
test_func_2_specific_name: str = "test_functions_2_empty_input_param()"
test_func_3_specific_name: str = "test_functions_3_empty_input_param()"
test_func_4_specific_name: str = "test_functions_4_empty_input_param()"


# Define column constant for get_functions
get_functions_col_name: typing.List[str] = [
    "FUNCTION_CAT",
    "FUNCTION_SCHEM",
    "FUNCTION_NAME",
    "REMARKS",
    "FUNCTION_TYPE",
    "SPECIFIC_NAME"
]


# Define result structure for get_functions
@dataclass
class FunctionInfo:
    function_cat: str
    function_schem: str
    function_name: str
    remarks: typing.Optional[str]
    function_type: int
    specific_name: str


# Define expected returned result for get_functions
get_functions_result: typing.List[FunctionInfo] = [
    FunctionInfo(current_catalog, test_schema_1, test_func_1, '', FunctionType.NOTABLE, test_func_1_specific_name),
    FunctionInfo(current_catalog, test_schema_1, test_func_2, '', FunctionType.NOTABLE, test_func_2_specific_name),
    FunctionInfo(current_catalog, test_schema_2, test_func_3, '', FunctionType.NOTABLE, test_func_3_specific_name),
    FunctionInfo(current_catalog, test_schema_2, test_func_4, '', FunctionType.NOTABLE, test_func_4_specific_name),
]


# Define column constant for get_function_columns
get_function_columns_col_name: typing.List[str] = [
    "FUNCTION_CAT",
    "FUNCTION_SCHEM",
    "FUNCTION_NAME",
    "COLUMN_NAME",
    "COLUMN_TYPE",
    "DATA_TYPE",
    "TYPE_NAME",
    "PRECISION",
    "LENGTH",
    "SCALE",
    "RADIX",
    "NULLABLE",
    "REMARKS",
    "CHAR_OCTET_LENGTH",
    "ORDINAL_POSITION",
    "IS_NULLABLE",
    "SPECIFIC_NAME"
]


# Define result structure for get_function_columns
@dataclass
class FunctionColumnInfo:
    function_cat: str
    function_schem: str
    function_name: str
    column_name: str
    column_type: int
    data_type: int
    type_name: str
    precision: int
    length: typing.Optional[int]
    scale: int
    radix: int
    nullable: int
    remarks: typing.Optional[str]
    char_octet_length: typing.Optional[int]
    ordinal_position: int
    is_nullable: str
    specific_name: str


# Define expected returned result for get_function_columns
get_function_columns_result: typing.List[FunctionColumnInfo] = [
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "", FunctionColumnType.RETURN, 4, "int4", 10, 4, 0, 10, 2, "", None, 0, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_smallint", FunctionColumnType.IN, 5, "int2", 5, 2, 0, 10, 2, "", None, 1, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_integer", FunctionColumnType.IN, 4, "int4", 10, 4, 0, 10, 2, "", None, 2, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_bigint", FunctionColumnType.IN, -5, "int8", 19, 20, 0, 10, 2, "", None, 3, "", test_func_1_specific_name),
    # SQL UDF has an issue where precision and scale was not stored and returned properly for numeric data type
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_numeric", FunctionColumnType.IN, 2, "numeric", 0, None, 0, 10, 2, "", None, 4, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_real", FunctionColumnType.IN, 7, "float4", 8, 4, 8, 10, 2, "", None, 5, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_double", FunctionColumnType.IN, 8, "float8", 17, 8, 17, 10, 2, "", None, 6, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_boolean", FunctionColumnType.IN, -7, "bool", 1, 1, 0, 10, 2, "", None, 7, "", test_func_1_specific_name),
    # SQL UDF has an issue where character_maximum_length was not stored and returned properly for char/varchar data type
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_char", FunctionColumnType.IN, 1, "char", 0, None, 0, 10, 2, "", None, 8, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_varchar", FunctionColumnType.IN, 12, "varchar", 0, None, 0, 10, 2, "", None, 9, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_date", FunctionColumnType.IN, 91, "date", 13, 6, 0, 10, 2, "", None, 10, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_1, "test_column_timestamp", FunctionColumnType.IN, 93, "timestamp", 29, 6, 6, 10, 2, "", None, 11, "", test_func_1_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_1, test_func_2, "", FunctionColumnType.RETURN, 4, "int4", 10, 4, 0, 10, 2, "", None, 0, "", test_func_2_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_2, test_func_3, "", FunctionColumnType.RETURN, 4, "int4", 10, 4, 0, 10, 2, "", None, 0, "", test_func_3_specific_name),
    FunctionColumnInfo(current_catalog, test_schema_2, test_func_4, "", FunctionColumnType.RETURN, 4, "int4", 10, 4, 0, 10, 2, "", None, 0, "", test_func_4_specific_name)
]


# Test environment setup Query
# Data type time, timetz, timestamptz, intervaly2m, intervald2s are not allowed in Function
startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS {} cascade;".format(test_schema_1),
    "DROP SCHEMA IF EXISTS {} cascade;".format(test_schema_2),
    "CREATE SCHEMA {}".format(test_schema_1),
    "CREATE SCHEMA {}".format(test_schema_2),
    "CREATE OR REPLACE FUNCTION {}.{}(test_column_smallint smallint, test_column_integer integer, test_column_bigint bigint, test_column_numeric numeric(10,5), test_column_real real, test_column_double double precision, test_column_boolean boolean, test_column_char char(20), test_column_varchar varchar(256), test_column_date date, test_column_timestamp timestamp) RETURNS int stable AS $$ select 1 $$ LANGUAGE sql;".format(test_schema_1, test_func_1),
    "CREATE OR REPLACE FUNCTION {}.{}() RETURNS int stable AS $$ select 1 $$ LANGUAGE sql;".format(test_schema_1, test_func_2),
    "CREATE OR REPLACE FUNCTION {}.{}() RETURNS int stable AS $$ select 1 $$ LANGUAGE sql;".format(test_schema_2, test_func_3),
    "CREATE OR REPLACE FUNCTION {}.{}() RETURNS int stable AS $$ select 1 $$ LANGUAGE sql;".format(test_schema_2, test_func_4)
)


# Test case constant
catalogs_case: typing.List[typing.Optional[str]] = [None, "%", "", current_catalog, current_catalog+"%", "wrong_database"]
schemas_case: typing.List[typing.Optional[str]]  = [test_schema_1, "test_functions_schema_%", "wrong_schema"]
functions_case: typing.List[typing.Optional[str]]  = ["%", "", test_func_1, "test%", "wrong_function"]
columns_case: typing.List[typing.Optional[str]]  = [None, "%", "", "test_column_smallint", "test_column_%", "wrong_column"]

class TestMetadataAPIFunctions:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_config(self, request, db_kwargs):
        setup_metadata_test_env(db_kwargs, current_catalog, startup_stmts)

        def fin():
            teardown_metadata_test_env(db_kwargs, current_catalog)

        request.addfinalizer(fin)

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("function", functions_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_functions(self, db_kwargs, catalog, schema, function, is_single_database_metadata) -> None:
        expected_result: typing.List[FunctionInfo] = self.get_functions_matches(catalog, schema, function)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_functions",
            method_args=(catalog, schema, function),
            expected_col_name=get_functions_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("function", functions_case)
    @pytest.mark.parametrize("columns", columns_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_function_columns(self, db_kwargs, catalog, schema, function, columns, is_single_database_metadata) -> None:
        expected_result: typing.List[FunctionColumnInfo] = self.get_function_columns_matches(catalog, schema, function, columns)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_function_columns",
            method_args=(catalog, schema, function, columns),
            expected_col_name=get_function_columns_col_name,
            expected_result=expected_result
        )

    @staticmethod
    def get_functions_matches(catalog, schema, function) -> typing.List[FunctionInfo]:
        result: typing.List[FunctionInfo] = []
        for func_info in get_functions_result:
            if ((exact_matches(catalog, func_info.function_cat)) and
                    (pattern_matches(schema, func_info.function_schem)) and
                    (pattern_matches(function, func_info.function_name))):
                result.append(func_info)
        return result

    @staticmethod
    def get_function_columns_matches(catalog, schema, function, column) -> typing.List[FunctionColumnInfo]:
        result: typing.List[FunctionColumnInfo] = []
        for func_col_info in get_function_columns_result:
            if ((exact_matches(catalog, func_col_info.function_cat)) and
                    (pattern_matches(schema, func_col_info.function_schem)) and
                    (pattern_matches(function, func_col_info.function_name)) and
                    (pattern_matches(column, func_col_info.column_name))):
                result.append(func_col_info)
        return result
