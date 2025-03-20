import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from unittest.mock import Mock, PropertyMock, mock_open, patch, MagicMock, call

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

cur_db_kwargs: typing.Dict

current_catalog: str = "test_catalog"

get_catalogs_index: int = 0
get_schemas_index: int = 1
get_tables_index: int = 2
get_columns_index: int = 3

col_label_index: int = 0

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

col_name = [
    ["TABLE_CAT"],
    ["TABLE_SCHEM", "TABLE_CATALOG"],
    ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"],
    ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"]
]

old_col_name = [
    ["table_cat"],
    ["table_schem", "table_catalog"],
    ["table_cat", "table_schem", "table_name", "table_type", "remarks", "type_cat", "type_schem", "type_name", "self_referencing_col_name", "ref_generation"],
    ["table_cat", "table_schem", "table_name", "column_name", "data_type", "type_name", "column_size", "buffer_length", "decimal_digits", "num_prec_radix", "nullable", "remarks", "column_def", "sql_data_type", "sql_datetime_sub", "char_octet_length", "ordinal_position", "is_nullable", "scope_catalog", "scope_schema", "scope_table", "source_data_type", "is_autoincrement", "is_generatedcolumn"]
]

get_catalog_result = [
    [current_catalog],
]

get_schema_result = [
    ["test_schema_1",current_catalog],
    ["test_schema_2",current_catalog]
]

get_table_result = [
    [current_catalog, "test_schema_1", "test_table_1", "TABLE",None,"","","","",""],
    [current_catalog, "test_schema_1", "test_table_2", "TABLE",None,"","","","",""],
    [current_catalog, "test_schema_2", "test_table_3", "TABLE",None,"","","","",""],
    [current_catalog, "test_schema_2", "test_table_4", "TABLE",None,"","","","",""]
]

get_column_result = [
    [current_catalog, "test_schema_1", "test_table_1", "test_column_smallint"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_integer"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_integer_auto"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_bigint"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_bigint_auto"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_numeric"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_real"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_double"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_boolean"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_char"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_varchar"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_date"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_time"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_timetz"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_timestamp"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_timestamptz"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_intervaly2m"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_intervald2s"],
    [current_catalog, "test_schema_1", "test_table_1", "test_column_intervald2s_cus"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_smallint"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_integer"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_integer_auto"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_bigint"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_bigint_auto"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_numeric"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_real"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_double"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_boolean"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_char"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_varchar"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_date"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_time"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_timetz"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_timestamp"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_timestamptz"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_intervaly2m"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_intervald2s"],
    [current_catalog, "test_schema_1", "test_table_2", "test_column_intervald2s_cus"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_smallint"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_integer"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_integer_auto"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_bigint"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_bigint_auto"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_numeric"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_real"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_double"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_boolean"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_char"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_varchar"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_date"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_time"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_timetz"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_timestamp"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_timestamptz"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_intervaly2m"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_intervald2s"],
    [current_catalog, "test_schema_2", "test_table_3", "test_column_intervald2s_cus"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_smallint"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_integer"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_integer_auto"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_bigint"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_bigint_auto"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_numeric"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_real"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_double"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_boolean"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_char"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_varchar"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_date"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_time"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_timetz"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_timestamp"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_timestamptz"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_intervaly2m"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_intervald2s"],
    [current_catalog, "test_schema_2", "test_table_4", "test_column_intervald2s_cus"]
]

catalogs_case = [None, "%", "", current_catalog, current_catalog+"%", "wrong_database"]
schemas_case = ["test_schema_1", "test_schema_%", "wrong_schema"]
tables_case = [None, "%", "", "test_table_1", "test%", "wrong_table"]
columns_case = [None, "%", "", "test_column_smallint", "test_column_%", "wrong_column"]
tables_types_case = [
    [],
    ["TABLE", "VIEW"],
    ["TAB"]
]

@pytest.fixture(scope="class", autouse=True)
def test_metadataAPI_config(request, db_kwargs):
    global cur_db_kwargs
    cur_db_kwargs = dict(db_kwargs)

    with redshift_connector.connect(**cur_db_kwargs) as con:
        con.paramstyle = "format"
        con.autocommit = True
        with con.cursor() as cursor:
            try:
                cursor.execute("drop database {}".format(current_catalog))
            except redshift_connector.ProgrammingError:
                pass
            cursor.execute("create database {}".format(current_catalog))
    cur_db_kwargs["database"] = current_catalog
    with redshift_connector.connect(**cur_db_kwargs) as con:
        con.paramstyle = "format"
        with con.cursor() as cursor:
            for stmt in startup_stmts:
                cursor.execute(stmt)

            con.commit()
    def fin():
        try:
            with redshift_connector.connect(**db_kwargs) as con:
                con.autocommit = True
                with con.cursor() as cursor:
                    cursor.execute("drop database {}".format(current_catalog))
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)


def test_get_catalogs() -> None:
    global cur_db_kwargs
    with redshift_connector.connect(**cur_db_kwargs) as conn:
        with conn.cursor() as cursor:
            with patch(
                    "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
            ) as mock_is_single_database_metadata:
                mock_is_single_database_metadata.__get__ = Mock(return_value=True)

                result: tuple = cursor.get_catalogs()

                column = cursor.description
                if cursor.supportSHOWDiscovery() >= 2:
                    assert len(col_name[get_catalogs_index]) == len(column)

                    for col1, col2 in zip(col_name[get_catalogs_index], column):
                        assert col1 == col2[col_label_index]
                else:
                    assert len(old_col_name[get_catalogs_index]) == len(column)

                    for col1, col2 in zip(old_col_name[get_catalogs_index], column):
                        assert col1 == col2[col_label_index]

                assert len(result) == len(get_catalog_result)

                for row1, row2 in zip(result, get_catalog_result):
                    assert len(row1) == len(row2)
                    for rs1, rs2 in zip(row1, row2):
                        assert rs1 == rs2

def test_get_catalogs_multiple() -> None:
    global cur_db_kwargs
    with redshift_connector.connect(**cur_db_kwargs) as conn:
        with conn.cursor() as cursor:
            with patch(
                    "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
            ) as mock_is_single_database_metadata:
                mock_is_single_database_metadata.__get__ = Mock(return_value=False)

                result: tuple = cursor.get_catalogs()

                column = cursor.description
                if cursor.supportSHOWDiscovery() >= 2:
                    assert len(col_name[get_catalogs_index]) == len(column)

                    for col1, col2 in zip(col_name[get_catalogs_index], column):
                        assert col1 == col2[col_label_index]
                else:
                    assert len(old_col_name[get_catalogs_index]) == len(column)

                    for col1, col2 in zip(old_col_name[get_catalogs_index], column):
                        assert col1 == col2[col_label_index]

                assert len(result) >= len(get_catalog_result)

@pytest.mark.parametrize("catalog", catalogs_case)
@pytest.mark.parametrize("schema", schemas_case)
def test_get_schemas(catalog, schema) -> None:
    global cur_db_kwargs
    with redshift_connector.connect(**cur_db_kwargs) as conn:
        with conn.cursor() as cursor:
            with patch(
                    "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
            ) as mock_is_single_database_metadata:
                mock_is_single_database_metadata.__get__ = Mock(return_value=True)

                result: tuple = cursor.get_schemas(catalog, schema)

                column = cursor.description
                if cursor.supportSHOWDiscovery() >= 2:
                    assert len(col_name[get_schemas_index]) == len(column)

                    for col1, col2 in zip(col_name[get_schemas_index], column):
                        assert col1 == col2[col_label_index]
                else:
                    assert len(old_col_name[get_schemas_index]) == len(column)

                    for col1, col2 in zip(old_col_name[get_schemas_index], column):
                        assert col1 == col2[col_label_index]

                expected_res = get_schemas_matches(catalog, schema)
                assert len(result) == len(expected_res)

                for row1, row2 in zip(result, expected_res):
                    assert len(row1) == len(row2)
                    for rs1, rs2 in zip(row1, row2):
                        assert rs1 == rs2

@pytest.mark.parametrize("catalog", catalogs_case)
@pytest.mark.parametrize("schema", schemas_case)
@pytest.mark.parametrize("table", tables_case)
@pytest.mark.parametrize("table_type", tables_types_case)
def test_get_tables(catalog, schema, table, table_type) -> None:
    global cur_db_kwargs
    with redshift_connector.connect(**cur_db_kwargs) as conn:
        with conn.cursor() as cursor:
            with patch(
                    "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
            ) as mock_is_single_database_metadata:
                mock_is_single_database_metadata.__get__ = Mock(return_value=True)

                try:
                    result: tuple = cursor.get_tables(catalog, schema, table, table_type)
                    column = cursor.description
                    if cursor.supportSHOWDiscovery() >= 2:
                        assert len(col_name[get_tables_index]) == len(column)

                        for col1, col2 in zip(col_name[get_tables_index], column):
                            assert col1 == col2[col_label_index]
                    else:
                        assert len(old_col_name[get_tables_index]) == len(column)

                        for col1, col2 in zip(old_col_name[get_tables_index], column):
                            assert col1 == col2[col_label_index]

                    expected_res = get_tables_matches(catalog, schema, table, table_type)

                    assert len(result) == len(expected_res)

                    for row1, row2 in zip(result, expected_res):
                        assert len(row1) == len(row2)
                        for rs1, rs2 in zip(row1, row2):
                            assert rs1 == rs2
                except Exception as e:
                    assert "Invalid type" in e.args[0]

@pytest.mark.parametrize("catalog", catalogs_case)
@pytest.mark.parametrize("schema", schemas_case)
@pytest.mark.parametrize("table", tables_case)
@pytest.mark.parametrize("columns", columns_case)
def test_get_columns(catalog, schema, table, columns) -> None:
    global cur_db_kwargs
    with redshift_connector.connect(**cur_db_kwargs) as conn:
        with conn.cursor() as cursor:
            with patch(
                    "redshift_connector.Connection.is_single_database_metadata", new_callable=PropertyMock()
            ) as mock_is_single_database_metadata:
                mock_is_single_database_metadata.__get__ = Mock(return_value=True)

                result: tuple = cursor.get_columns(catalog, schema, table, columns)

                column = cursor.description
                if cursor.supportSHOWDiscovery() >= 2:
                    assert len(col_name[get_columns_index]) == len(column)

                    for col1, col2 in zip(col_name[get_columns_index], column):
                        assert col1 == col2[col_label_index]
                else:
                    assert len(old_col_name[get_columns_index]) == len(column)

                    for col1, col2 in zip(old_col_name[get_columns_index], column):
                        assert col1 == col2[col_label_index]

                expected_res = get_columns_matches(catalog, schema, table, columns)

                assert len(result) == len(expected_res)

                for row1, row2 in zip(result, expected_res):
                    assert row1[0] == row2[0]
                    assert row1[1] == row2[1]
                    assert row1[2] == row2[2]
                    assert row1[3] == row2[3]

def get_schemas_matches(catalog, schema) -> typing.List[typing.List]:
    result = []
    for row in get_schema_result:
        if matches_catalog(catalog, row[1]) and matches(schema, row[0]):
            result.append(row)
    return result

def get_tables_matches(catalog, schema, table, table_type) -> typing.List[typing.List]:
    result = []
    for row in get_table_result:
        if (matches_catalog(catalog, row[0]) and matches(schema, row[1]) and matches(table, row[2])) and (table_type is None or len(table_type) == 0 or row[3] in table_type):
            result.append(row)
    return result

def get_columns_matches(catalog, schema, table, column) -> typing.List[typing.List]:
    result = []
    for row in get_column_result:
        if matches_catalog(catalog, row[0]) and matches(schema, row[1]) and matches(table, row[2]) and matches(column, row[3]):
            result.append(row)
    return result

def matches(str1: str, str2: str) -> bool:
    return str1 is None or str1 == '' or str1 =='%' or str1 == str2 or ('%' in str1 and str2.startswith('test'))

def matches_catalog(str1: str, str2: str) -> bool:
    return str1 is None or str1 == '' or str1 == str2