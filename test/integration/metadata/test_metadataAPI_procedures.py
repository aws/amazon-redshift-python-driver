import typing
import pytest  # type: ignore
import redshift_connector
from dataclasses import dataclass
from enum import IntEnum
from metadata_test_utils import setup_metadata_test_env, teardown_metadata_test_env, pattern_matches, exact_matches, run_metadata_test, ProcedureType, ProcedureColumnType


# Define constant
is_single_database_metadata_case: typing.List[bool] = [True, False]
current_catalog: str = "test_procedures_catalog"
test_schema_1: str = "test_procedures_schema_1"
test_schema_2: str = "test_procedures_schema_2"
test_proc_1: str = "test_procedures_1_input_param"
test_proc_2: str = "test_procedures_2_no_input_param"
test_proc_3: str = "test_procedures_3_input_output_param"
test_proc_4: str = "test_procedures_4_output_param"
test_proc_1_specific_name: str = "test_procedures_1_input_param(smallint, integer, bigint, numeric, real, double precision, boolean, character, character varying, date, time without time zone, time with time zone, timestamp without time zone, timestamp with time zone, intervaly2m, intervald2s)"
test_proc_2_specific_name: str = "test_procedures_2_no_input_param()"
test_proc_3_specific_name: str = "test_procedures_3_input_output_param(integer)"
test_proc_4_specific_name: str = "test_procedures_4_output_param()"


# Define column constant for get_procedures
get_procedures_col_name: typing.List[str] = [
    "PROCEDURE_CAT",
    "PROCEDURE_SCHEM",
    "PROCEDURE_NAME",
    "RESERVE1",
    "RESERVE2",
    "RESERVE3",
    "REMARKS",
    "PROCEDURE_TYPE",
    "SPECIFIC_NAME"
]


# Define result structure for get_procedures
@dataclass
class ProcedureInfo:
    procedure_cat: str
    procedure_schem: str
    procedure_name: str
    reserve1: typing.Optional[str]
    reserve2: typing.Optional[str]
    reserve3: typing.Optional[str]
    remarks: str
    procedure_type: int
    specific_name: str


# Define expected returned result for get_procedures
get_procedures_result: typing.List[ProcedureInfo] = [
    ProcedureInfo(current_catalog, test_schema_1, test_proc_1, None, None, None, '', ProcedureType.NO_RESULT, test_proc_1_specific_name),
    ProcedureInfo(current_catalog, test_schema_1, test_proc_2, None, None, None, '', ProcedureType.NO_RESULT, test_proc_2_specific_name),
    ProcedureInfo(current_catalog, test_schema_2, test_proc_3, None, None, None, '', ProcedureType.RETURNS_RESULT, test_proc_3_specific_name),
    ProcedureInfo(current_catalog, test_schema_2, test_proc_4, None, None, None, '', ProcedureType.RETURNS_RESULT, test_proc_4_specific_name)
]


# Define column constant for get_procedure_columns
get_procedure_columns_col_name: typing.List[str] = [
    "PROCEDURE_CAT",
    "PROCEDURE_SCHEM",
    "PROCEDURE_NAME",
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
    "COLUMN_DEF",
    "SQL_DATA_TYPE",
    "SQL_DATETIME_SUB",
    "CHAR_OCTET_LENGTH",
    "ORDINAL_POSITION",
    "IS_NULLABLE",
    "SPECIFIC_NAME"
]


# Define result structure for get_procedure_columns
@dataclass
class ProcedureColumnInfo:
    procedure_cat: str
    procedure_schem: str
    procedure_name: str
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
    column_def: typing.Optional[str]
    sql_data_type: int
    sql_datetime_sub: typing.Optional[int]
    char_octet_length: typing.Optional[int]
    ordinal_position: int
    is_nullable: str
    specific_name: str


# Define expected returned result for get_procedure_columns
get_procedure_columns_result: typing.List[ProcedureColumnInfo] = [
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_smallint", ProcedureColumnType.IN, 5, "int2", 5, 2, 0, 10, 2, "", None, 5, None, None, 1, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_integer", ProcedureColumnType.IN, 4, "int4", 10, 4, 0, 10, 2, "", None, 4, None, None, 2, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_bigint", ProcedureColumnType.IN, -5, "int8", 19, 20, 0, 10, 2, "", None, -5, None, None, 3, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_numeric", ProcedureColumnType.IN, 2, "numeric", 10, None, 5, 10, 2, "", None, 2, None, None, 4, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_real", ProcedureColumnType.IN, 7, "float4", 8, 4, 8, 10, 2, "", None, 7, None, None, 5, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_double", ProcedureColumnType.IN, 8, "float8", 17, 8, 17, 10, 2, "", None, 8, None, None, 6, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_boolean", ProcedureColumnType.IN, -7, "bool", 1, 1, 0, 10, 2, "", None, -7, None, None, 7, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_char", ProcedureColumnType.IN, 1, "char", 20, None, 0, 10, 2, "", None, 1, None, None, 8, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_varchar", ProcedureColumnType.IN, 12, "varchar", 256, None, 0, 10, 2, "", None, 12, None, None, 9, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_date", ProcedureColumnType.IN, 91, "date", 13, 6, 0, 10, 2, "", None, 91, None, None, 10, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_time", ProcedureColumnType.IN, 92, "time", 15, 15, 6, 10, 2, "", None, 92, None, None, 11, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_timetz", ProcedureColumnType.IN, 2013, "timetz", 21, 21, 6, 10, 2, "", None, 2013, None, None, 12, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_timestamp", ProcedureColumnType.IN, 93, "timestamp", 29, 6, 6, 10, 2, "", None, 93, None, None, 13, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_timestamptz", ProcedureColumnType.IN, 2014, "timestamptz", 35, 35, 6, 10, 2, "", None, 2014, None, None, 14, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_intervaly2m", ProcedureColumnType.IN, 1111, "intervaly2m", 32, 4, 0, 10, 2, "", None, 1111, None, None, 15, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_1, test_proc_1, "test_column_intervald2s", ProcedureColumnType.IN, 1111, "intervald2s", 64, 8, 6, 10, 2, "", None, 1111, None, None, 16, "", test_proc_1_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_2, test_proc_3, "test_column_input", ProcedureColumnType.IN, 4, "int4", 10, 4, 0, 10, 2, "", None, 4, None, None, 1, "", test_proc_3_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_2, test_proc_3, "test_column_result", ProcedureColumnType.OUT, 4, "int4", 10, 4, 0, 10, 2, "", None, 4, None, None, 2, "", test_proc_3_specific_name),
    ProcedureColumnInfo(current_catalog, test_schema_2, test_proc_4, "test_column_result", ProcedureColumnType.OUT, 4, "int4", 10, 4, 0, 10, 2, "", None, 4, None, None, 1, "", test_proc_4_specific_name)
]


# Test environment setup Query
startup_stmts: typing.Tuple[str, ...] = (
    "DROP SCHEMA IF EXISTS {} cascade;".format(test_schema_1),
    "DROP SCHEMA IF EXISTS {} cascade;".format(test_schema_2),
    "CREATE SCHEMA {}".format(test_schema_1),
    "CREATE SCHEMA {}".format(test_schema_2),
    "CREATE OR REPLACE PROCEDURE {}.{}(test_column_smallint smallint, test_column_integer integer, test_column_bigint bigint, test_column_numeric numeric(10,5), test_column_real real, test_column_double double precision, test_column_boolean boolean, test_column_char char(20), test_column_varchar varchar(256), test_column_date date, test_column_time time, test_column_timetz timetz, test_column_timestamp timestamp, test_column_timestamptz timestamptz, test_column_intervaly2m interval year to month, test_column_intervald2s interval day to second) AS $$ BEGIN select 1; END; $$ LANGUAGE plpgsql;".format(test_schema_1, test_proc_1),
    "CREATE OR REPLACE PROCEDURE {}.{}() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;".format(test_schema_1, test_proc_2),
    "CREATE OR REPLACE PROCEDURE {}.{}(test_column_input IN int, test_column_result OUT int) AS $$ BEGIN IF input > 10 THEN test_column_result := 1; ELSE RAISE INFO 'No result'; END IF; END; $$ LANGUAGE plpgsql;".format(test_schema_2, test_proc_3),
    "CREATE OR REPLACE PROCEDURE {}.{}(test_column_result OUT int) AS $$ BEGIN test_column_result := 1; END; $$ LANGUAGE plpgsql;".format(test_schema_2, test_proc_4)
)


# Test case constant
catalogs_case: typing.List[typing.Optional[str]] = [None, "%", "", current_catalog, current_catalog+"%", "wrong_database"]
schemas_case: typing.List[typing.Optional[str]] = [test_schema_1, "test_procedures_schema_%", "wrong_schema"]
procedures_case: typing.List[typing.Optional[str]] = ["%", "", test_proc_1, "test%", "wrong_procedure"]
columns_case: typing.List[typing.Optional[str]] = [None, "%", "", "test_column_smallint", "test_column_%", "wrong_column"]


class TestMetadataAPIProcedures:
    @pytest.fixture(scope="class", autouse=True)
    def test_metadataAPI_config(self, request, db_kwargs):
        setup_metadata_test_env(db_kwargs, current_catalog, startup_stmts)

        def fin():
            teardown_metadata_test_env(db_kwargs, current_catalog)

        request.addfinalizer(fin)

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("procedure", procedures_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_procedures(self, db_kwargs, catalog, schema, procedure, is_single_database_metadata) -> None:
        expected_result: typing.List[ProcedureInfo] = self.get_procedures_matches(catalog, schema, procedure)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_procedures",
            method_args=(catalog, schema, procedure),
            expected_col_name=get_procedures_col_name,
            expected_result=expected_result
        )

    @pytest.mark.parametrize("catalog", catalogs_case)
    @pytest.mark.parametrize("schema", schemas_case)
    @pytest.mark.parametrize("procedure", procedures_case)
    @pytest.mark.parametrize("column", columns_case)
    @pytest.mark.parametrize("is_single_database_metadata", is_single_database_metadata_case)
    def test_get_procedure_columns(self, db_kwargs, catalog, schema, procedure, column, is_single_database_metadata) -> None:
        expected_result: typing.List[ProcedureColumnInfo] = self.get_procedure_columns_matches(catalog, schema, procedure, column)

        run_metadata_test(
            db_kwargs=db_kwargs,
            current_catalog=current_catalog,
            is_single_database_metadata=is_single_database_metadata,
            min_show_discovery_version=3,
            method_name="get_procedure_columns",
            method_args=(catalog, schema, procedure, column),
            expected_col_name=get_procedure_columns_col_name,
            expected_result=expected_result
        )

    @staticmethod
    def get_procedures_matches(catalog, schema, procedure) -> typing.List[ProcedureInfo]:
        result: typing.List[ProcedureInfo] = []
        for proc_info in get_procedures_result:
            if ((exact_matches(catalog, proc_info.procedure_cat)) and
                    (pattern_matches(schema, proc_info.procedure_schem)) and
                    (pattern_matches(procedure, proc_info.procedure_name))):
                result.append(proc_info)
        return result

    @staticmethod
    def get_procedure_columns_matches(catalog, schema, procedure, column) -> typing.List[ProcedureColumnInfo]:
        result: typing.List[ProcedureColumnInfo] = []
        for proc_col_info in get_procedure_columns_result:
            if ((exact_matches(catalog, proc_col_info.procedure_cat)) and
                    (pattern_matches(schema, proc_col_info.procedure_schem)) and
                    (pattern_matches(procedure, proc_col_info.procedure_name)) and
                    (pattern_matches(column, proc_col_info.column_name))):
                result.append(proc_col_info)
        return result
