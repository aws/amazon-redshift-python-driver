import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

# Manually set the following flag to False if we want to test cross-database metadata api call
disable_cross_database_testing: bool = True

current_catalog: str = "təst_cat好log pattern"
identifier: str = "test_n𝒜m好e123 pattern"

name_list: typing.List[str] = [
    "\"test_n𝒜m好e123 pattern\"",
    "\"test\_n𝒜m好e123 pattern\"",
    "\"test_\\n𝒜m好e123 pattern\"",
    "\"test\\\\_n𝒜m好e123 pattern\"",
    "\"test_\\\\n𝒜m好e123 pattern\"",
    "\"test%n𝒜m好e123 pattern\"",
    "\"test\%n𝒜m好e123 pattern\"",
    "\"test%\\n𝒜m好e123 pattern\"",
    "\"test\\\\%n𝒜m好e123 pattern\"",
    "\"test%\\\\n𝒜m好e123 pattern\"",
    "\"test__n𝒜m好e123 pattern\"",
    "\"test%%n𝒜m好e123 pattern\"",
    "\"test\\_\\_n𝒜m好e123 pattern\""
]

startup_stmts: typing.Tuple[str, ...] = (
    "CREATE SCHEMA {};".format(name_list[0]),
    "CREATE SCHEMA {};".format(name_list[1]),
    "CREATE SCHEMA {};".format(name_list[2]),
    "CREATE SCHEMA {};".format(name_list[3]),
    "CREATE SCHEMA {};".format(name_list[4]),
    "CREATE SCHEMA {};".format(name_list[5]),
    "CREATE SCHEMA {};".format(name_list[6]),
    "CREATE SCHEMA {};".format(name_list[7]),
    "CREATE SCHEMA {};".format(name_list[8]),
    "CREATE SCHEMA {};".format(name_list[9]),
    "CREATE SCHEMA {};".format(name_list[10]),
    "CREATE SCHEMA {};".format(name_list[11]),
    "CREATE SCHEMA {};".format(name_list[12]),

    "create table {}.{} ({} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT, {} INT);".format(name_list[0], name_list[0], name_list[0], name_list[1], name_list[2], name_list[3], name_list[4], name_list[5], name_list[6], name_list[7], name_list[8], name_list[9], name_list[10], name_list[11], name_list[12]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[1]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[2]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[3]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[4]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[5]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[6]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[7]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[8]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[9]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[10]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[11]),
    "create table {}.{} (Col1 INT);".format(name_list[0], name_list[12]),

)

test_cases = [
    # 1.1 Pattern matching for single character
    ("test_n𝒜m好e123 pattern", 2,
     ["test%n𝒜m好e123 pattern",
      "test_n𝒜m好e123 pattern"],
     ["test_n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern"]
     ),

    # 1.2 Exact match for underscore
    ("test\\_n𝒜m好e123 pattern", 1,
     ["test_n𝒜m好e123 pattern"],
     ["test_n𝒜m好e123 pattern"]),

    # 1.3 Pattern match for backslash following by single character
    ("test\\\\_n𝒜m好e123 pattern", 2,
     ["test\\%n𝒜m好e123 pattern",
      "test\\_n𝒜m好e123 pattern"],
     ["test\\_n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern"]
     ),

    # 1.4 Exact match for backslash following by underscore
    ("test\\\\\\_n𝒜m好e123 pattern", 1,
     ["test\\_n𝒜m好e123 pattern"],
     ["test\\_n𝒜m好e123 pattern"]),

    # 1.5 Pattern match for 2 backslash following by single character
    ("test\\\\\\\\_n𝒜m好e123 pattern", 2,
     ["test\\\\%n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern"],
     ["test\\\\_n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern"]),

    # 1.6 Exact match for 2 backslash following by underscore
    ("test\\\\\\\\\\_n𝒜m好e123 pattern", 1,
     ["test\\\\_n𝒜m好e123 pattern"],
     ["test\\\\_n𝒜m好e123 pattern"]),

    # 2.1 Pattern match for any character
    ("test%", 13,
     ["test%%n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern",
      "test\\_n𝒜m好e123 pattern",
      "test_\\\\n𝒜m好e123 pattern",
      "test_\\n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern",
      "test_n𝒜m好e123 pattern"],
     ["test_n𝒜m好e123 pattern",
      "test\\_n𝒜m好e123 pattern",
      "test_\\n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test_\\\\n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern",
      "test%%n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern"]),

    # 2.2 Exact match for percentage
    ("test\\%n𝒜m好e123 pattern", 1,
     ["test%n𝒜m好e123 pattern"],
     ["test%n𝒜m好e123 pattern"]),

    # 2.3 Pattern match for backslash following by any character
    ("test\\\\%n𝒜m好e123 pattern", 5,
     ["test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern",
      "test\\_n𝒜m好e123 pattern"],
     ["test\\_n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern"]
     ),

    # 2.4 Exact match for backslash following by percentage
    ("test\\\\\\%n𝒜m好e123 pattern", 1,
     ["test\\%n𝒜m好e123 pattern"],
     ["test\\%n𝒜m好e123 pattern"]),

    # 2.5 Pattern match for 2 backslash following by any character
    ("test\\\\\\\\%n𝒜m好e123 pattern", 2,
     ["test\\\\%n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern"],
     ["test\\\\_n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern"]
     ),

    # 2.6 Exact match for 2 backslash following by percentage
    ("test\\\\\\\\\\%n𝒜m好e123 pattern", 1,
     ["test\\\\%n𝒜m好e123 pattern"],
     ["test\\\\%n𝒜m好e123 pattern"]),

    # 3.1 Pattern match for two character
    ("test__n𝒜m好e123 pattern", 6,
     ["test%%n𝒜m好e123 pattern",
     "test%\\n𝒜m好e123 pattern",
     "test\\%n𝒜m好e123 pattern",
     "test\\_n𝒜m好e123 pattern",
     "test_\\n𝒜m好e123 pattern",
     "test__n𝒜m好e123 pattern"],
     ["test\\_n𝒜m好e123 pattern",
      "test_\\n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern",
      "test%%n𝒜m好e123 pattern"]
     ),

    # 3.2 Pattern match for underscore following by single character
    ("test\\__n𝒜m好e123 pattern", 2,
     ["test_\\n𝒜m好e123 pattern",
     "test__n𝒜m好e123 pattern"],
     ["test_\\n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern"]),

    # 3.3 Pattern match for single character following by an underscore
    ("test_\\_n𝒜m好e123 pattern", 2,
     ["test\\_n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern"],
     ["test\\_n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern"]),

    # 3.4 Exact match for double underscore
    ("test\\_\\_n𝒜m好e123 pattern", 1,
     ["test__n𝒜m好e123 pattern"],
     ["test__n𝒜m好e123 pattern"]),

    # 3.5 Pattern match for backslash following by single character and underscore
    ("test\\\\_\\_n𝒜m好e123 pattern", 1,
     ["test\\\\_n𝒜m好e123 pattern"],
     ["test\\\\_n𝒜m好e123 pattern"]),

    # 3.6 Pattern match for underscore following by backslash and single character
    ("test\\_\\\\_n𝒜m好e123 pattern", 1,
     ["test_\\\\n𝒜m好e123 pattern"],
     ["test_\\\\n𝒜m好e123 pattern"]),

    # 3.7 Pattern match for backslash following by single character and backslash and single character
    ("test\\\\_\\\\_n𝒜m好e123 pattern", 1,
     ["test\\_\\_n𝒜m好e123 pattern"],
     ["test\\_\\_n𝒜m好e123 pattern"]),

    # 4.1 Pattern match for any character
    ("test%%n𝒜m好e123 pattern", 13,
     ["test%%n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern",
      "test\\_n𝒜m好e123 pattern",
      "test_\\\\n𝒜m好e123 pattern",
      "test_\\n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern",
      "test_n𝒜m好e123 pattern"],
     ["test_n𝒜m好e123 pattern",
      "test\\_n𝒜m好e123 pattern",
      "test_\\n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test_\\\\n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern",
      "test__n𝒜m好e123 pattern",
      "test%%n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern"]),

    # 4.2 Pattern match for percentage following by any character
    ("test\\%%n𝒜m好e123 pattern", 4,
     ["test%%n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern"],
     ["test%n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern",
      "test%%n𝒜m好e123 pattern"]),

    # 4.3 Pattern match for any character following by a percentage
    ("test%\\%n𝒜m好e123 pattern", 4,
     ["test%%n𝒜m好e123 pattern",
      "test%n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern"],
     ["test%n𝒜m好e123 pattern",
      "test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test%%n𝒜m好e123 pattern"]),

    # 4.4 Exact match for double percentage
    ("test\\%\\%n𝒜m好e123 pattern", 1,
     ["test%%n𝒜m好e123 pattern"],
     ["test%%n𝒜m好e123 pattern"]),

    # 4.5 Pattern match for backslash following by any character and percentage
    ("test\\\\%\\%n𝒜m好e123 pattern", 2,
     ["test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern"],
     ["test\\%n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern"]),

    # 4.6 Pattern match for percentage following by backslash and any character
    ("test\\%\\\\%n𝒜m好e123 pattern", 2,
     ["test%\\\\n𝒜m好e123 pattern",
      "test%\\n𝒜m好e123 pattern"],
     ["test%\\n𝒜m好e123 pattern",
      "test%\\\\n𝒜m好e123 pattern"]),

    # 4.7 Pattern match for backslash following by any character and backslash and any character
    ("test\\\\%\\\\%n𝒜m好e123 pattern", 3,
     ["test\\\\%n𝒜m好e123 pattern",
      "test\\\\_n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern"],
     ["test\\\\_n𝒜m好e123 pattern",
      "test\\\\%n𝒜m好e123 pattern",
      "test\\_\\_n𝒜m好e123 pattern"]
     ),
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
                cursor.execute("drop database \"{}\";".format(current_catalog))
            except redshift_connector.ProgrammingError:
                pass
            cursor.execute("create database \"{}\";".format(current_catalog))
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
                    cursor.execute("drop database \"{}\";".format(current_catalog))
                    cursor.execute("select 1;")
        except redshift_connector.ProgrammingError:
            pass

    request.addfinalizer(fin)

@pytest.mark.parametrize("schema, expected_count, expected_result, expected_col_result", test_cases)
def test_get_schemas_pattern_matching(db_kwargs, schema, expected_count, expected_result, expected_col_result) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_schemas(current_catalog, schema)

            assert len(result) == expected_count
            for actual_row, expected_row in zip(result, expected_result):
                assert actual_row[0] == expected_row

@pytest.mark.parametrize("table, expected_count, expected_result, expected_col_result", test_cases)
def test_get_tables_pattern_matching(db_kwargs, table, expected_count, expected_result, expected_col_result) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    print(test_db_kwargs["database"])
    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_tables(current_catalog, identifier, table, [])

            assert len(result) == expected_count
            for actual_row, expected_row in zip(result, expected_result):
                assert actual_row[2] == expected_row

@pytest.mark.parametrize("column, expected_count, expected_result, expected_col_result", test_cases)
def test_get_columns_pattern_matching(db_kwargs, column, expected_count, expected_result, expected_col_result) -> None:
    global cur_db_kwargs
    if disable_cross_database_testing:
        test_db_kwargs = dict(cur_db_kwargs)
    else:
        test_db_kwargs = dict(db_kwargs)
        test_db_kwargs["database_metadata_current_db_only"] = False

    with redshift_connector.connect(**test_db_kwargs) as conn:
        with conn.cursor() as cursor:
            result: tuple = cursor.get_columns(current_catalog, identifier, identifier, column)

            assert len(result) == expected_count
            for actual_row, expected_row in zip(result, expected_col_result):
                assert actual_row[3] == expected_row