import logging
import typing

from redshift_connector.metadataAPIHelper import MetadataAPIHelper

_logger: logging.Logger = logging.getLogger(__name__)


class MetadataServerAPIHelper(MetadataAPIHelper):
    def __init__(self: "MetadataServerAPIHelper", cursor_instance) -> None:
        super().__init__()
        self._cursor = cursor_instance

    # Helper function for metadata API get_catalogs to return intermediate result for post-processing
    def get_catalog_server_api(self, catalog: str = None) -> typing.Tuple:
        _logger.debug("Calling Server API SHOW DATABASES")

        # Build the query for SHOW API call
        sql: str = ""
        if self.check_name_is_not_pattern(catalog):
            sql = self._sql_show_databases
        else:
            sql = self._sql_show_databases_like.format(catalog)

        # Execute SHOW DATABASES
        self._cursor.execute(sql)

        # Fetch the result for SHOW DATABASES
        catalogs: typing.Tuple = self._cursor.fetchall()

        # Create Column name / Column Index mapping for SHOW DATABASES
        if self._cursor._SHOW_DATABASES_Col_index is None:
            self._cursor._SHOW_DATABASES_Col_index = self.build_column_name_index_map()

        _logger.debug("Successfully executed SHOW DATABASE for catalog = %s", catalog)

        return catalogs

    def get_schema_server_api(self, catalog: str = None, schema_pattern: str = None, ret_empty: bool = False, isSingleDatabaseMetaData: bool = True) -> typing.List[typing.Tuple]:
        _logger.debug("Calling Server API SHOW SCHEMAS")

        intermediate_rs: typing.List[typing.Tuple] = []

        if not ret_empty:
            # Skip SHOW DATABASES API call if catalog name is specified
            catalog_list: typing.List = []
            if self.check_name_is_exact_name(catalog):
                catalog_list.append(catalog)
            else:
                catalog_list = self.get_catalog_list(catalog, isSingleDatabaseMetaData)

            for cur_catalog in catalog_list:
                intermediate_rs.append(self.call_show_schema(cur_catalog, schema_pattern))

                # Create Column name / Column Index mapping for SHOW SCHEMAS
                if self._cursor._SHOW_SCHEMAS_Col_index is None:
                    self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

            _logger.debug("Successfully executed SHOW SCHEMAS for catalog = %s, schemaPattern = %s", catalog, schema_pattern)

        return intermediate_rs

    def get_table_server_api(self, catalog: str = "%", schema_pattern: str = "%", table_name_pattern: str = "%", ret_empty: bool = False, isSingleDatabaseMetaData: bool = True) -> typing.List[typing.Tuple]:
        _logger.debug("Calling Server API SHOW TABLES")

        intermediate_rs: typing.List[typing.Tuple] = []

        if not ret_empty:
            # Skip SHOW DATABASES API call if catalog name is specified
            if self.check_name_is_exact_name(catalog):
                # Skip SHOW SCHEMAS API call if schema name is specified
                if self.check_name_is_exact_name(schema_pattern):
                    intermediate_rs.append(self.call_show_table(catalog, schema_pattern, table_name_pattern))

                    # Create Column name / Column Index mapping for SHOW TABLES
                    if self._cursor._SHOW_TABLES_Col_index is None:
                        self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()

                else:
                    show_schemas_rs: typing.Tuple = self.call_show_schema(catalog, schema_pattern)

                    # Create Column name / Column Index mapping for SHOW SCHEMAS
                    if self._cursor._SHOW_SCHEMAS_Col_index is None:
                        self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

                    for cur_schema in show_schemas_rs:
                        intermediate_rs.append(self.call_show_table(catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], table_name_pattern))

                        # Create Column name / Column Index mapping for SHOW TABLES
                        if self._cursor._SHOW_TABLES_Col_index is None:
                            self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()

            else:
                catalog_list: typing.List = self.get_catalog_list(catalog, isSingleDatabaseMetaData)

                for cur_catalog in catalog_list:
                    # Can't Skip SHOW SCHEMA if catalog is not specified to prevent calling SHOW TABLES
                    # with catalog which required additional permission
                    show_schemas_rs: typing.Tuple = self.call_show_schema(cur_catalog, schema_pattern)

                    # Create Column name / Column Index mapping for SHOW SCHEMAS
                    if self._cursor._SHOW_SCHEMAS_Col_index is None:
                        self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

                    for cur_schema in show_schemas_rs:
                        intermediate_rs.append(self.call_show_table(cur_catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], table_name_pattern))

                        # Create Column name / Column Index mapping for SHOW TABLES
                        if self._cursor._SHOW_TABLES_Col_index is None:
                            self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()



            _logger.debug("Successfully executed SHOW TABLES for catalog = %s, schemaPattern = %s, tableNamePattern = %s", catalog, schema_pattern, table_name_pattern)

        return intermediate_rs


    def get_column_server_api(self, catalog: str = "%", schema_pattern: str = "%", table_name_pattern: str = "%", column_name_pattern: str = "%", ret_empty: bool = False, isSingleDatabaseMetaData: bool = True) -> typing.List[typing.Tuple]:
        _logger.debug("Calling Server API SHOW COLUMNS")

        intermediate_rs: typing.List[typing.Tuple] = []

        if not ret_empty:
            # Skip SHOW DATABASES API call if catalog name is specified
            if self.check_name_is_exact_name(catalog):
                # Skip SHOW SCHEMAS API call if schema name is specified
                if self.check_name_is_exact_name(schema_pattern):
                    # Skip SHOW TABLES API call if table name is specified
                    if self.check_name_is_exact_name(table_name_pattern):
                        intermediate_rs.append(self.call_show_column(catalog, schema_pattern, table_name_pattern, column_name_pattern))

                        # Create Column name / Column Index mapping for SHOW COLUMNS
                        if self._cursor._SHOW_COLUMNS_Col_index is None:
                            self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()

                    else:
                        show_tables_rs: typing.Tuple = self.call_show_table(catalog, schema_pattern, table_name_pattern)

                        # Create Column name / Column Index mapping for SHOW TABLES
                        if self._cursor._SHOW_TABLES_Col_index is None:
                            self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()

                        for cur_table in show_tables_rs:
                            intermediate_rs.append(self.call_show_column(catalog, schema_pattern, cur_table[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]], column_name_pattern))

                            # Create Column name / Column Index mapping for SHOW COLUMNS
                            if self._cursor._SHOW_COLUMNS_Col_index is None:
                                self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()

                else:
                    show_schemas_rs: typing.Tuple = self.call_show_schema(catalog, schema_pattern)

                    # Create Column name / Column Index mapping for SHOW SCHEMAS
                    if self._cursor._SHOW_SCHEMAS_Col_index is None:
                        self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

                    for cur_schema in show_schemas_rs:
                        # Skip SHOW TABLES API call if table name is specified
                        if self.check_name_is_exact_name(table_name_pattern):
                            intermediate_rs.append(self.call_show_column(catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], table_name_pattern, column_name_pattern))

                            # Create Column name / Column Index mapping for SHOW COLUMNS
                            if self._cursor._SHOW_COLUMNS_Col_index is None:
                                self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()

                        else:
                            show_tables_rs: typing.Tuple = self.call_show_table(catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], table_name_pattern)

                            # Create Column name / Column Index mapping for SHOW TABLES
                            if self._cursor._SHOW_TABLES_Col_index is None:
                                self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()

                            for cur_table in show_tables_rs:
                                intermediate_rs.append(self.call_show_column(catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], cur_table[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]], column_name_pattern))

                                # Create Column name / Column Index mapping for SHOW COLUMNS
                                if self._cursor._SHOW_COLUMNS_Col_index is None:
                                    self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()

            else:
                catalog_list: typing.List = self.get_catalog_list(catalog, isSingleDatabaseMetaData)

                for cur_catalog in catalog_list:
                    # Can't Skip SHOW SCHEMA if catalog is not specified to prevent calling SHOW TABLES
                    # with catalog which required additional permission
                    show_schemas_rs: typing.Tuple = self.call_show_schema(cur_catalog, schema_pattern)

                    # Create Column name / Column Index mapping for SHOW SCHEMAS
                    if self._cursor._SHOW_SCHEMAS_Col_index is None:
                        self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

                    for cur_schema in show_schemas_rs:
                        # Skip SHOW TABLES API call if table name is specified
                        if self.check_name_is_exact_name(table_name_pattern):
                            intermediate_rs.append(self.call_show_column(cur_catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], table_name_pattern, column_name_pattern))

                            # Create Column name / Column Index mapping for SHOW COLUMNS
                            if self._cursor._SHOW_COLUMNS_Col_index is None:
                                self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()
                        else:
                            show_tables_rs: typing.Tuple = self.call_show_table(cur_catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], table_name_pattern)

                            # Create Column name / Column Index mapping for SHOW TABLES
                            if self._cursor._SHOW_TABLES_Col_index is None:
                                self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()

                            for cur_table in show_tables_rs:
                                intermediate_rs.append(self.call_show_column(cur_catalog, cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]], cur_table[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]], column_name_pattern))

                                # Create Column name / Column Index mapping for SHOW COLUMNS
                                if self._cursor._SHOW_COLUMNS_Col_index is None:
                                    self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()


            _logger.debug("Successfully executed SHOW COLUMNS for catalog = %s, schema = %s, tableName = %s, columnNamePattern = %s", catalog, schema_pattern, table_name_pattern, column_name_pattern)

        return intermediate_rs

    # Helper function to return a list of catalog
    def get_catalog_list(self, catalog: str, isSingleDatabaseMetaData: bool) -> typing.List:
        catalog_list: typing.List = []
        catalog_rs: typing.Tuple = self.get_catalog_server_api(catalog)
        cur_catalog: str = self._cursor.cur_catalog()

        for rs in catalog_rs:
            if isSingleDatabaseMetaData is True:
                if rs[0] == cur_catalog:
                    catalog_list.append(rs[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]])
            else:
                catalog_list.append(rs[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]])

        return catalog_list

    # Helper function to call SHOW SCHEMAS
    def call_show_schema(self, catalog: str, schema: str) -> typing.Tuple:
        # Build the query for SHOW API call
        sql: str = ""
        if self.check_name_is_not_pattern(schema):
            sql = self._sql_show_schemas.format(catalog)
        else:
            sql = self._sql_show_schemas_like.format(catalog, schema)

        # Execute SHOW SCHEMAS
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    # Helper function to call SHOW TABLES
    def call_show_table(self, catalog: str, schema: str, table: str) -> typing.Tuple:
        # Build the query for SHOW API call
        sql: str = ""
        if self.check_name_is_not_pattern(table):
            sql = self._sql_show_tables.format(catalog, schema)
        else:
            sql = self._sql_show_tables_like.format(catalog, schema, table)

        # Execute SHOW TABLES
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    # Helper function to call SHOW COLUMNS
    def call_show_column(self, catalog: str, schema: str, table: str, column: str) -> typing.Tuple:
        # Build the query for SHOW API call
        sql: str = ""
        if self.check_name_is_not_pattern(column):
            sql = self._sql_show_columns.format(catalog, schema, table)
        else:
            sql = self._sql_show_columns_like.format(catalog, schema, table, column)

        # Execute SHOW COLUMNS
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    # Helper function to build column name/index mapping
    def build_column_name_index_map(self) -> typing.Dict:
        column_name_index_map = {}
        column = self._cursor.description
        for col, i in zip(column, range(len(column))):
            column_name_index_map[col[0]] = int(i)

        return column_name_index_map