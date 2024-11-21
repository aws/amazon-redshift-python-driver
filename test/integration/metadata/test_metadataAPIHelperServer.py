import configparser
import os
import typing
import pytest  # type: ignore

import redshift_connector
from redshift_connector.metadataAPIHelper import MetadataAPIHelper

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

@pytest.mark.skip
def test_SHOW_DATABASES_col(ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES;")

            column = cursor.description

            col_set: typing.Set = set()

            for col in column:
                col_set.add(col[0])

            mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

            assert mock_metadataAPIHelper._SHOW_DATABASES_database_name in col_set

@pytest.mark.skip
def test_SHOW_SCHEMAS_col(ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor: redshift_connector.Cursor = conn.cursor()
            cursor.execute("SHOW SCHEMAS FROM DATABASE test_dsdb;")

            column = cursor.description

            col_set: typing.Set = set()

            for col in column:
                col_set.add(col[0])

            mock_metadataAPIHelper: MetadataAPIHelper = MetadataAPIHelper()

            assert mock_metadataAPIHelper._SHOW_SCHEMA_database_name in col_set
            assert mock_metadataAPIHelper._SHOW_SCHEMA_schema_name in col_set

@pytest.mark.skip
def test_SHOW_TABLES_col(ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor: redshift_connector.Cursor = conn.cursor()
            cursor.execute("SHOW TABLES FROM SCHEMA test_dsdb.test_schema_1;")

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

@pytest.mark.skip
def test_SHOW_COLUMNS_col(ds_consumer_dsdb_kwargs) -> None:
    del ds_consumer_dsdb_kwargs["extra"]
    with redshift_connector.connect(**ds_consumer_dsdb_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor: redshift_connector.Cursor = conn.cursor()
            cursor.execute("SHOW COLUMNS FROM TABLE test_dsdb.test_schema_1.test_table_1;")

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


