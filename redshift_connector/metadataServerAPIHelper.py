import logging
import typing

from redshift_connector.metadataAPIHelper import MetadataAPIHelper

_logger: logging.Logger = logging.getLogger(__name__)


class MetadataServerAPIHelper(MetadataAPIHelper):
    def __init__(self: "MetadataServerAPIHelper", cursor_instance) -> None:
        super().__init__()
        self._cursor = cursor_instance

    def get_catalog_server_api(self) -> typing.Tuple:
        """
        Helper function for metadata API get_catalogs to return intermediate result for post-processing

        Returns
        -------
        A tuple containing result set for SHOW DATABASES: tuple
        """
        _logger.debug("Calling Server API SHOW DATABASES")

        # Execute SHOW DATABASES
        self._cursor.execute(self._sql_show_databases)

        # Fetch the result for SHOW DATABASES
        catalogs: typing.Tuple = self._cursor.fetchall()

        # Create Column name / Column Index mapping for SHOW DATABASES
        if self._cursor._SHOW_DATABASES_Col_index is None:
            self._cursor._SHOW_DATABASES_Col_index = self.build_column_name_index_map()

        _logger.debug("Successfully executed SHOW DATABASE")

        return catalogs

    def get_schema_server_api(self, catalog: str = None, schema_pattern: str = None, ret_empty: bool = False, isSingleDatabaseMetaData: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_schemas to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        ret_empty : Optional[bool] Whether or not to return empty result
        isSingleDatabaseMetaData : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW SCHEMAS: list
        """
        intermediate_rs: typing.List[typing.Tuple] = []

        if not ret_empty:
            # Get catalog list
            catalog_list: typing.List = self.call_get_catalog_list(catalog, isSingleDatabaseMetaData)

            for cur_catalog in catalog_list:
                intermediate_rs.append(self.call_show_schema(cur_catalog, schema_pattern))

                # Create Column name / Column Index mapping for SHOW SCHEMAS
                if self._cursor._SHOW_SCHEMAS_Col_index is None:
                    self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

            _logger.debug("Successfully executed SHOW SCHEMAS for catalog = %s, schemaPattern = %s", catalog, schema_pattern)

        return intermediate_rs

    def get_table_server_api(self, catalog: str = None, schema_pattern: str = None, table_name_pattern: str = None, ret_empty: bool = False, isSingleDatabaseMetaData: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_tables to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        table_name_pattern : Optional[str] A valid pattern for desired table names
        ret_empty : Optional[bool] Whether or not to return empty result
        isSingleDatabaseMetaData : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW TABLES: list
        """

        intermediate_rs: typing.List[typing.Tuple] = []

        if not ret_empty:
            # Get catalog list
            catalog_list: typing.List = self.call_get_catalog_list(catalog, isSingleDatabaseMetaData)

            for cur_catalog in catalog_list:
                #Get schema list
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


    def get_column_server_api(self, catalog: str = None, schema_pattern: str = None, table_name_pattern: str = None, column_name_pattern: str = None, ret_empty: bool = False, isSingleDatabaseMetaData: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_columns to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        table_name_pattern : Optional[str] A valid pattern for desired table names
        column_name_pattern : Optional[str] A valid pattern for desired column names
        ret_empty : Optional[bool] Whether or not to return empty result
        isSingleDatabaseMetaData : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW COLUMNS: list
        """

        intermediate_rs: typing.List[typing.Tuple] = []

        if not ret_empty:
            # Get catalog list
            catalog_list: typing.List = self.call_get_catalog_list(catalog, isSingleDatabaseMetaData)

            for cur_catalog in catalog_list:
                # Get schema list
                show_schemas_rs: typing.Tuple = self.call_show_schema(cur_catalog, schema_pattern)

                # Create Column name / Column Index mapping for SHOW SCHEMAS
                if self._cursor._SHOW_SCHEMAS_Col_index is None:
                    self._cursor._SHOW_SCHEMAS_Col_index = self.build_column_name_index_map()

                for cur_schema in show_schemas_rs:
                    # Get table list
                    cur_schema_name: str = cur_schema[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]]
                    show_tables_rs: typing.Tuple = self.call_show_table(cur_catalog, cur_schema_name, table_name_pattern)

                    # Create Column name / Column Index mapping for SHOW TABLES
                    if self._cursor._SHOW_TABLES_Col_index is None:
                        self._cursor._SHOW_TABLES_Col_index = self.build_column_name_index_map()

                    for cur_table in show_tables_rs:
                        intermediate_rs.append(self.call_show_column(cur_catalog, cur_schema_name, cur_table[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]], column_name_pattern))

                        # Create Column name / Column Index mapping for SHOW COLUMNS
                        if self._cursor._SHOW_COLUMNS_Col_index is None:
                            self._cursor._SHOW_COLUMNS_Col_index = self.build_column_name_index_map()

            _logger.debug("Successfully executed SHOW COLUMNS for catalog = %s, schema = %s, tableName = %s, columnNamePattern = %s", catalog, schema_pattern, table_name_pattern, column_name_pattern)

        return intermediate_rs

    def call_get_catalog_list(self, catalog: str, isSingleDatabaseMetaData: bool) -> typing.List[typing.Tuple]:
        """
        Helper function to get a list of catalog name

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        isSingleDatabaseMetaData : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list of catalog: list
        """

        catalog_list: typing.List = []
        if not self.is_none_or_empty(catalog):
            if isSingleDatabaseMetaData is True:
                if catalog == self._cursor.cur_catalog():
                    catalog_list.append(catalog)
            else:
                catalog_list.append(catalog)
        else:
            catalog_list = self.get_catalog_list(isSingleDatabaseMetaData)
        return catalog_list

    def get_catalog_list(self, isSingleDatabaseMetaData: bool) -> typing.List:
        """
        Helper function to get a list of catalog name from SHOW DATABASES

        Parameters
        ----------
        isSingleDatabaseMetaData : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list of catalog from SHOW DATABASES: list
        """
        catalog_list: typing.List = []
        catalog_rs: typing.Tuple = self.get_catalog_server_api()
        cur_catalog: str = self._cursor.cur_catalog()

        for rs in catalog_rs:
            if isSingleDatabaseMetaData is True:
                if rs[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]] == cur_catalog:
                    catalog_list.append(rs[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]])
            else:
                catalog_list.append(rs[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]])

        return catalog_list

    def call_show_schema(self, catalog: str, schema: str) -> typing.Tuple:
        """
        Helper function to determine whether calling SHOW SCHEMAS with LIKE or not

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas

        Returns
        -------
        A tuple containing result set for SHOW SCHEMAS: tuple
        """
        if self.is_none_or_empty(schema):
            return self.call_show_schema_without_like(catalog)
        else:
            return self.call_show_schema_with_like(catalog, schema)

    def call_show_schema_with_like(self, catalog: str, schema: str) -> typing.Tuple:
        """
        Helper function to call SHOW SCHEMAS with LIKE

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas

        Returns
        -------
        A tuple containing result set for SHOW SCHEMAS: tuple
        """
        sql: str = self._sql_show_schemas_like.format(self.quote_ident(catalog), self.quote_literal(schema))
        _logger.debug("Calling Server API: %s", sql)

        # Execute SHOW SCHEMAS

        self._cursor.execute(sql)
        return self._cursor.fetchall()

    def call_show_schema_without_like(self, catalog: str) -> typing.Tuple:
        """
        Helper function to call SHOW SCHEMAS

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog

        Returns
        -------
        A tuple containing result set for SHOW SCHEMAS: tuple
        """
        sql: str = self._sql_show_schemas.format(self.quote_ident(catalog))
        _logger.debug("Calling Server API: %s", sql)

        # Execute SHOW SCHEMAS
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    # Helper function to call SHOW TABLES
    def call_show_table(self, catalog: str, schema: str, table: str) -> typing.Tuple:
        """
        Helper function to determine whether calling SHOW TABLES with LIKE or not

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas
        table : Optional[str] A valid pattern for desired table names

        Returns
        -------
        A tuple containing result set for SHOW TABLES: tuple
        """
        if self.is_none_or_empty(table):
            return self.call_show_table_without_like(catalog, schema)
        else:
            return self.call_show_table_with_like(catalog, schema, table)

    def call_show_table_with_like(self, catalog: str, schema: str, table: str) -> typing.Tuple:
        """
        Helper function to call SHOW TABLES with LIKE

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas
        table : Optional[str] A valid pattern for desired table names

        Returns
        -------
        A tuple containing result set for SHOW TABLES: tuple
        """
        sql: str = self._sql_show_tables_like.format(self.quote_ident(catalog), self.quote_ident(schema), self.quote_literal(table))
        _logger.debug("Calling Server API: %s", sql)

        # Execute SHOW TABLES
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    def call_show_table_without_like(self, catalog: str, schema: str) -> typing.Tuple:
        """
        Helper function to call SHOW TABLE

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas

        Returns
        -------
        A tuple containing result set for SHOW TABLES: tuple
        """
        sql: str = self._sql_show_tables.format(self.quote_ident(catalog), self.quote_ident(schema))
        _logger.debug("Calling Server API: %s", sql)

        # Execute SHOW TABLES
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    # Helper function to call SHOW COLUMNS
    def call_show_column(self, catalog: str, schema: str, table: str, column: str) -> typing.Tuple:
        """
        Helper function to determine whether calling SHOW COLUMNS with LIKE or not

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas
        table : Optional[str] A valid pattern for desired table names
        column : Optional[str] A valid pattern for desired column names

        Returns
        -------
        A tuple containing result set for SHOW COLUMNS: tuple
        """
        if self.is_none_or_empty(column):
            return self.call_show_column_without_like(catalog, schema, table)
        else:
            return self.call_show_column_with_like(catalog, schema, table, column)

    def call_show_column_with_like(self, catalog: str, schema: str, table: str, column: str) -> typing.Tuple:
        """
        Helper function to call SHOW COLUMNS with LIKE

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas
        table : Optional[str] A valid pattern for desired table names
        column : Optional[str] A valid pattern for desired column names

        Returns
        -------
        A tuple containing result set for SHOW COLUMNS: tuple
        """
        sql: str = self._sql_show_columns_like.format(self.quote_ident(catalog), self.quote_ident(schema), self.quote_ident(table), self.quote_literal(column))
        _logger.debug("Calling Server API: %s", sql)

        # Execute SHOW COLUMNS
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    def call_show_column_without_like(self, catalog: str, schema: str, table: str) -> typing.Tuple:
        """
        Helper function to call SHOW COLUMNS

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] A valid pattern for desired schemas
        table : Optional[str] A valid pattern for desired table names

        Returns
        -------
        A tuple containing result set for SHOW COLUMNS: tuple
        """
        sql: str = self._sql_show_columns.format(self.quote_ident(catalog), self.quote_ident(schema), self.quote_ident(table))
        _logger.debug("Calling Server API: %s", sql)

        # Execute SHOW COLUMNS
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    def quote_ident(self, input_str: str) -> str:
        """
        Helper function to call QUOTE_IDENT and return result as string

        Parameters
        ----------
        input_str : A string to be quoted

        Returns
        -------
        A quoted identifier: str
        """
        self._cursor.execute(self._prepare_quote_ident, [input_str])
        result: tuple = self._cursor.fetchall()
        return result[self._quote_iden_result_row][self._quote_iden_result_col]

    def quote_literal(self, input_str: str) -> str:
        """
        Helper function to call QUOTE_LITERAL and return result as string

        Parameters
        ----------
        input_str : A string to be quoted

        Returns
        -------
        A quoted literal: str
        """
        self._cursor.execute(self._prepare_quote_literal, [input_str])
        result: tuple = self._cursor.fetchall()
        return result[self._quote_literal_result_row][self._quote_literal_result_col]

    def build_column_name_index_map(self) -> typing.Dict:
        """
        Helper function to build column name/index mapping

        Returns
        -------
        A dictionary containing mapping between column name and column index: dict
        """
        column_name_index_map = {}
        column = self._cursor.description
        for col, i in zip(column, range(len(column))):
            column_name_index_map[col[self._row_description_col_label_index]] = int(i)

        return column_name_index_map