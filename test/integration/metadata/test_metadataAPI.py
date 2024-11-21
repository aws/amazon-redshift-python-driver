import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

col_name = [
    ["TABLE_CAT"],
    ["TABLE_SCHEM", "TABLE_CATALOG"],
    ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"],
    ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"]
]

get_catalog_result = [
    ["awsdatacatalog"],
    ["dev"],
    ["test_catalog_1"],
    ["test_catalog_2"],
    ["test_dsdb"]
]

get_schema_result = [
    ["test_schema_1","test_dsdb"],
    ["test_schema_2","test_dsdb"]
]

get_table_result = [
    ["test_dsdb", "test_schema_1", "test_table_1", "TABLE","","","","","",""],
    ["test_dsdb", "test_schema_1", "test_table_2", "TABLE","","","","","",""],
    ["test_dsdb", "test_schema_2", "test_table_3", "TABLE","","","","","",""],
    ["test_dsdb", "test_schema_2", "test_table_4", "TABLE","","","","","",""]
]

get_column_result = [
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_smallint"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_integer"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_integer_auto"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_bigint"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_bigint_auto"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_numeric"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_real"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_double"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_boolean"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_char"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_varchar"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_date"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_time"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_timetz"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_timestamp"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_timestamptz"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_intervaly2m"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_intervald2s"],
    ["test_dsdb", "test_schema_1", "test_table_1", "test_column_intervald2s_cus"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_smallint"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_integer"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_integer_auto"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_bigint"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_bigint_auto"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_numeric"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_real"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_double"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_boolean"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_char"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_varchar"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_date"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_time"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_timetz"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_timestamp"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_timestamptz"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_intervaly2m"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_intervald2s"],
    ["test_dsdb", "test_schema_1", "test_table_2", "test_column_intervald2s_cus"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_smallint"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_integer"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_integer_auto"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_bigint"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_bigint_auto"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_numeric"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_real"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_double"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_boolean"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_char"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_varchar"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_date"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_time"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_timetz"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_timestamp"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_timestamptz"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_intervaly2m"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_intervald2s"],
    ["test_dsdb", "test_schema_2", "test_table_3", "test_column_intervald2s_cus"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_smallint"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_integer"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_integer_auto"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_bigint"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_bigint_auto"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_numeric"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_real"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_double"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_boolean"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_char"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_varchar"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_date"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_time"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_timetz"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_timestamp"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_timestamptz"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_intervaly2m"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_intervald2s"],
    ["test_dsdb", "test_schema_2", "test_table_4", "test_column_intervald2s_cus"]
]

catalogs_case = [None, "%", "", "test_dsdb", "test%", "wrong_database"]
schemas_case = [None, "%", '', "test_schema_1", "test%", "wrong_schema"]
tables_case = [None, "%", "", "test_table_1", "test%", "wrong_table"]
columns_case = [None, "%", "", "test_column_smallint", "test_column_%", "wrong_column"]
tables_types_case = [
    None,
    ["TABLE", "VIEW"],
    ["TAB"]
]

@pytest.mark.skip
def test_get_catalogs(ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_catalogs()

            column = cursor.description
            assert len(col_name[0]) == len(column)

            for col1, col2 in zip(col_name[0], column):
                assert col1 == col2[0]

            assert len(result) == len(get_catalog_result)

            for row1, row2 in zip(result, get_catalog_result):
                assert len(row1) == len(row2)
                for rs1, rs2 in zip(row1, row2):
                    assert rs1 == rs2

@pytest.mark.skip
@pytest.mark.parametrize("catalog", catalogs_case)
@pytest.mark.parametrize("schema", schemas_case)
def test_get_schemas(catalog, schema, ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_schemas(catalog, schema)

            column = cursor.description
            assert len(col_name[1]) == len(column)

            for col1, col2 in zip(col_name[1], column):
                assert col1 == col2[0]

            expected_res = get_schemas_matches(catalog, schema)

            assert len(result) == len(expected_res)

            for row1, row2 in zip(result, expected_res):
                assert len(row1) == len(row2)
                for rs1, rs2 in zip(row1, row2):
                    assert rs1 == rs2

@pytest.mark.skip
@pytest.mark.parametrize("catalog", catalogs_case)
@pytest.mark.parametrize("schema", schemas_case)
@pytest.mark.parametrize("table", tables_case)
@pytest.mark.parametrize("table_type", tables_types_case)
def test_get_tables(catalog, schema, table, table_type, ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_tables(catalog, schema, table, table_type)

            column = cursor.description
            assert len(col_name[2]) == len(column)

            for col1, col2 in zip(col_name[2], column):
                assert col1 == col2[0]

            expected_res = get_tables_matches(catalog, schema, table, table_type)

            assert len(result) == len(expected_res)

            for row1, row2 in zip(result, expected_res):
                assert len(row1) == len(row2)
                for rs1, rs2 in zip(row1, row2):
                    assert rs1 == rs2

@pytest.mark.skip
@pytest.mark.parametrize("catalog", catalogs_case)
@pytest.mark.parametrize("schema", schemas_case)
@pytest.mark.parametrize("table", tables_case)
@pytest.mark.parametrize("columns", columns_case)
def test_get_columns(catalog, schema, table, columns, ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_columns(catalog, schema, table, columns)

            column = cursor.description
            assert len(col_name[3]) == len(column)

            for col1, col2 in zip(col_name[3], column):
                assert col1 == col2[0]

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
        if matches(catalog, row[1]) and matches(schema, row[0]):
            result.append(row)
    return result

def get_tables_matches(catalog, schema, table, table_type) -> typing.List[typing.List]:
    result = []
    for row in get_table_result:
        if (matches(catalog, row[0]) and matches(schema, row[1]) and matches(table, row[2])) and (table_type is None or row[3] in table_type):
            result.append(row)
    return result

def get_columns_matches(catalog, schema, table, column) -> typing.List[typing.List]:
    result = []
    for row in get_column_result:
        if matches(catalog, row[0]) and matches(schema, row[1]) and matches(table, row[2]) and matches(column, row[3]):
            result.append(row)
    return result

def matches(str1: str, str2: str) -> bool:
    return str1 is None or str1 == '' or str1 =='%' or str1 == str2 or ('%' in str1 and str2.startswith('test'))