import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")


def test_SHOW_DATABASES_col(db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            if cursor.supportSHOWDiscovery() >= 2:
                cursor.execute("SHOW DATABASES;")

                column = cursor.description

                col_set: typing.Set = set()

                for col in column:
                    col_set.add(col[0])

                mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

                assert mock_metadataAPIHelper._SHOW_DATABASES_database_name in col_set


def test_SHOW_SCHEMAS_col(db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            if cursor.supportSHOWDiscovery() >= 2:
                cursor.execute("SHOW SCHEMAS FROM DATABASE test_catalog;")

                column = cursor.description

                col_set: typing.Set = set()

                for col in column:
                    col_set.add(col[0])

                mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

                assert mock_metadataAPIHelper._SHOW_SCHEMA_database_name in col_set
                assert mock_metadataAPIHelper._SHOW_SCHEMA_schema_name in col_set


def test_SHOW_TABLES_col(db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            if cursor.supportSHOWDiscovery() >= 2:
                cursor.execute("SHOW TABLES FROM SCHEMA test_catalog.test_schema;")

                column = cursor.description

                col_set: typing.Set = set()

                for col in column:
                    col_set.add(col[0])

                mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

                assert mock_metadataAPIHelper._SHOW_TABLES_database_name in col_set
                assert mock_metadataAPIHelper._SHOW_TABLES_schema_name in col_set
                assert mock_metadataAPIHelper._SHOW_TABLES_table_name in col_set
                assert mock_metadataAPIHelper._SHOW_TABLES_table_type in col_set
                assert mock_metadataAPIHelper._SHOW_TABLES_remarks in col_set


def test_SHOW_COLUMNS_col(db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            if cursor.supportSHOWDiscovery() >= 2:
                cursor.execute("SHOW COLUMNS FROM TABLE test_catalog.test_schema.test_table;")

                column = cursor.description

                col_set: typing.Set = set()

                for col in column:
                    col_set.add(col[0])

                mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

                assert mock_metadataAPIHelper._SHOW_COLUMNS_database_name in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_schema_name in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_table_name in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_column_name in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_ordinal_position in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_column_default in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_is_nullable in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_data_type in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_character_maximum_length in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_numeric_precision in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_numeric_scale in col_set
                assert mock_metadataAPIHelper._SHOW_COLUMNS_remarks in col_set


