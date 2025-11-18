import logging
import typing

from redshift_connector.metadataAPIHelper import MetadataAPIHelper
from redshift_connector.error import (
    MISSING_MODULE_ERROR_MSG,
    InterfaceError,
    ProgrammingError,
)

_logger: logging.Logger = logging.getLogger(__name__)

class MetadataServerProxy(MetadataAPIHelper):
    def __init__(self: "MetadataServerProxy", cursor_instance) -> None:
        super().__init__()
        self._cursor = cursor_instance

    def get_catalogs(self) -> typing.Tuple:
        """
        Helper function for metadata API get_catalogs to return intermediate result for post-processing

        Returns
        -------
        A tuple containing result set for SHOW DATABASES: tuple
        """
        _logger.debug("Calling Server API SHOW DATABASES")

        catalogs: typing.Tuple = self.call_show_databases()
        self._ensure_column_index('_SHOW_DATABASES_Col_index')

        _logger.debug("Successfully executed SHOW DATABASE")

        return catalogs

    def get_schemas(self, catalog: str = None, schema_pattern: str = None, is_single_database_metadata: bool = True) -> \
    typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_schemas to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW SCHEMAS: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            sql = self._sql_show_schemas if self.is_none_or_empty(schema_pattern) else self._sql_show_schemas_like
            intermediate_rs.append(self.call_show_metadata(sql, [cur_catalog], schema_pattern))

            # Create Column name / Column Index mapping for SHOW SCHEMAS
            self._ensure_column_index('_SHOW_SCHEMAS_Col_index')

        _logger.debug("Successfully executed SHOW SCHEMAS for catalog = %s, schemaPattern = %s", catalog, schema_pattern)

        return intermediate_rs

    def get_tables(self, catalog: str = None, schema_pattern: str = None, table_name_pattern: str = None,
                   is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_tables to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        table_name_pattern : Optional[str] A valid pattern for desired table names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW TABLES: list
        """

        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                sql = self._sql_show_tables if self.is_none_or_empty(table_name_pattern) else self._sql_show_tables_like
                intermediate_rs.append(self.call_show_metadata(sql, [cur_catalog, cur_schema], table_name_pattern))

                # Create Column name / Column Index mapping for SHOW TABLES
                self._ensure_column_index('_SHOW_TABLES_Col_index')

        _logger.debug("Successfully executed SHOW TABLES for catalog = %s, schemaPattern = %s, tableNamePattern = %s", catalog, schema_pattern, table_name_pattern)

        return intermediate_rs

    def get_columns(self, catalog: str = None, schema_pattern: str = None, tablename_pattern: str = None,
                    columnname_pattern: str = None, is_single_database_metadata: bool = True) -> typing.List[
        typing.Tuple]:
        """
        Helper function for metadata API get_columns to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        tablename_pattern : Optional[str] A valid pattern for desired table names
        columnname_pattern : Optional[str] A valid pattern for desired column names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW COLUMNS: list
        """

        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                # Get table list
                table_list: typing.List = self.get_table_list(cur_catalog, cur_schema, tablename_pattern)

                for cur_table in table_list:
                    sql = self._sql_show_columns if self.is_none_or_empty(columnname_pattern) else self._sql_show_columns_like
                    intermediate_rs.append(self.call_show_metadata(sql, [cur_catalog, cur_schema, cur_table], columnname_pattern))


                    # Create Column name / Column Index mapping for SHOW COLUMNS
                    self._ensure_column_index('_SHOW_COLUMNS_Col_index')

        _logger.debug("Successfully executed SHOW COLUMNS for catalog = %s, schema = %s, tableName = %s, columnNamePattern = %s", catalog, schema_pattern, tablename_pattern, columnname_pattern)

        return intermediate_rs

    def get_primary_keys(self, catalog: str = None, schema: str = None, table: str = None,
                         is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_primary_keys to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] The name of the schema
        table : Optional[str] The name of the table
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW CONSTRAINTS: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = []
            if self.is_none_or_empty(schema):
                schema_list = self.get_schema_list(cur_catalog, schema)
            else:
                schema_list.append(schema)

            for cur_schema in schema_list:
                # Get table list
                table_list: typing.List = []
                if self.is_none_or_empty(table):
                    table_list = self.get_table_list(cur_catalog, cur_schema, table)
                else:
                    table_list.append(table)

                for cur_table in table_list:
                    intermediate_rs.append(self.call_show_metadata(self._sql_show_constraints_pk, [cur_catalog, cur_schema, cur_table]))

                    # Create Column name / Column Index mapping for SHOW CONSTRAINTS
                    self._ensure_column_index('_SHOW_CONSTRAINTS_PK_Col_index')

        _logger.debug("Successfully executed SHOW CONSTRAINTS for catalog = %s, schema = %s, table = %s", catalog, schema, table)

        return intermediate_rs

    def get_foreign_keys(self, catalog: str = None, schema: str = None, table: str = None,
                         is_single_database_metadata: bool = True, get_imported: bool = True) -> typing.List[
        typing.Tuple]:
        """
        Helper function for metadata API get_foreign_keys to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] The name of the schema
        table : Optional[str] The name of the table
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata
        get_imported : Optional[bool] Whether to get imported foreign keys

        Returns
        -------
        A list containing several result set for SHOW CONSTRAINTS: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = []
            if self.is_none_or_empty(schema):
                schema_list = self.get_schema_list(cur_catalog, schema)
            else:
                schema_list.append(schema)

            for cur_schema in schema_list:
                # Get table list
                table_list: typing.List = []
                if self.is_none_or_empty(table):
                    table_list = self.get_table_list(cur_catalog, cur_schema, table)
                else:
                    table_list.append(table)

                for cur_table in table_list:
                    sql: str
                    if get_imported:
                        sql = self._sql_show_constraints_fk
                    else:
                        sql = self._sql_show_constraints_fk_ex

                    intermediate_rs.append(self.call_show_metadata(sql, [cur_catalog, cur_schema, cur_table]))

                    # Create Column name / Column Index mapping for SHOW CONSTRAINTS
                    self._ensure_column_index('_SHOW_CONSTRAINTS_FK_Col_index')

        _logger.debug("Successfully executed SHOW CONSTRAINTS for catalog = %s, schema = %s, table = %s", catalog, schema, table)

        return intermediate_rs

    def get_best_row_identifier(self, catalog: str = None, schema: str = None, table: str = None,
                                is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_best_row_identifier to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] The name of the schema
        table : Optional[str] The name of the table
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for best row identifiers: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = []
            if self.is_none_or_empty(schema):
                schema_list = self.get_schema_list(cur_catalog, schema)
            else:
                schema_list.append(schema)

            for cur_schema in schema_list:
                # Get table list
                table_list: typing.List = []
                if self.is_none_or_empty(table):
                    table_list = self.get_table_list(cur_catalog, cur_schema, table)
                else:
                    table_list.append(table)

                for cur_table in table_list:
                    show_constraints_rs: typing.Tuple = self.call_show_metadata(self._sql_show_constraints_pk, [cur_catalog, cur_schema, cur_table])

                    # Create Column name / Column Index mapping for SHOW CONSTRAINTS
                    self._ensure_column_index('_SHOW_CONSTRAINTS_PK_Col_index')

                    for cur_rs in show_constraints_rs:
                        pk_column = cur_rs[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_column_name]]
                        cur_rs = self.call_show_metadata(self._sql_show_columns, [cur_catalog, cur_schema, cur_table])

                        # Create Column name / Column Index mapping for SHOW COLUMNS
                        self._ensure_column_index('_SHOW_COLUMNS_Col_index')

                        # Apply primary key column filter on the result set
                        for cur_row in cur_rs:
                            if pk_column == cur_row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_name]]:
                                intermediate_rs.append((cur_row, ))
                                break

        _logger.debug("Successfully executed SHOW CONSTRAINTS for catalog = %s, schema = %s, table = %s", catalog, schema, table)

        return intermediate_rs

    def get_column_privileges(self, catalog: str = None, schema: str = None, table: str = None,
                              column_name_pattern: str = None, is_single_database_metadata: bool = True) -> typing.List[
        typing.Tuple]:
        """
        Helper function for metadata API get_column_privileges to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema : Optional[str] The name of the schema
        table : Optional[str] The name of the table
        column_name_pattern : Optional[str] A valid pattern for desired column names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for column privileges: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = []
            if self.is_none_or_empty(schema):
                schema_list = self.get_schema_list(cur_catalog, schema)
            else:
                schema_list.append(schema)

            for cur_schema in schema_list:
                rs =  self.call_show_metadata(
                    self._sql_show_grant_column,
                    [cur_catalog, cur_schema, table]
                )

                # Create Column name / Column Index mapping for SHOW COLUMNS
                self._ensure_column_index('_SHOW_GRANTS_COLUMN_Col_index')

                # Since SHOW doesn't support LIKE clause, Driver need to handle pattern matching
                column_name_index: int = self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_column_name]
                if column_name_pattern:
                    rs = [row for row in rs if self.pattern_match(
                        row[column_name_index],
                        column_name_pattern
                    )]

                # Sort the result based on privilege type based on JDBC spec
                privilege_type_index: int = self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_privilege_type]
                sorted_rs = tuple(sorted(rs, key=lambda x: x[
                    privilege_type_index
                ]))
                intermediate_rs.append(sorted_rs)

        _logger.debug("Successfully executed SHOW GRANTS for catalog = %s, schema = %s, table = %s, column = %s", catalog, schema, table, column_name_pattern)

        return intermediate_rs

    def get_table_privileges(self, catalog: str = None, schema_pattern: str = None, table_name_pattern: str = None,
                             is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_table_privileges to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        table_name_pattern : Optional[str] A valid pattern for desired table names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for table privileges: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                # Get table list
                table_list: typing.List = self.get_table_list(cur_catalog, cur_schema, table_name_pattern)

                for cur_table in table_list:
                    rs: tuple = self.call_show_metadata(
                        self._sql_show_grant_table,
                        [cur_catalog, cur_schema, cur_table]
                    )

                    # Create Column name / Column Index mapping for SHOW GRANTS
                    self._ensure_column_index('_SHOW_GRANTS_TABLE_Col_index')

                    # Sort the result based on privilege type based on JDBC spec
                    privilege_type_index: int = self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_privilege_type]
                    sorted_rs = tuple(sorted(rs, key=lambda x: x[
                        privilege_type_index
                    ]))
                    intermediate_rs.append(sorted_rs)

        _logger.debug("Successfully executed SHOW GRANTS for catalog = %s, schema = %s, table = %s", catalog, schema_pattern, table_name_pattern)

        return intermediate_rs

    def get_procedures(self, catalog: str = None, schema_pattern: str = None, procedure_name_pattern: str = None,
                       is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_procedures to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        procedure_name_pattern : Optional[str] A valid pattern for desired procedure names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW PROCEDURES: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                sql = self._sql_show_procedures if self.is_none_or_empty(procedure_name_pattern) else self._sql_show_procedures_like
                intermediate_rs.append(self.call_show_metadata(sql, [cur_catalog, cur_schema], procedure_name_pattern))

                # Create Column name / Column Index mapping for SHOW PROCEDURES
                self._ensure_column_index('_SHOW_PROCEDURES_Col_index')

        _logger.debug("Successfully executed SHOW PROCEDURES for catalog = %s, schema_pattern = %s, procedure_name_pattern = %s", catalog, schema_pattern, procedure_name_pattern)

        return intermediate_rs

    def get_procedure_columns(self, catalog: str = None, schema_pattern: str = None, procedure_name_pattern: str = None,
                              column_name_pattern: str = None, is_single_database_metadata: bool = True) -> typing.List[
        typing.Tuple]:
        """
        Helper function for metadata API get_procedure_columns to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        procedure_name_pattern : Optional[str] A valid pattern for desired procedure names
        column_name_pattern : Optional[str] A valid pattern for desired column names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for procedure columns: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                # Get procedure list
                sql = self._sql_show_procedures if self.is_none_or_empty(procedure_name_pattern) else self._sql_show_procedures_like
                show_procedures_rs = self.call_show_metadata(sql, [cur_catalog, cur_schema], procedure_name_pattern)

                # Create Column name / Column Index mapping for SHOW PROCEDURES
                self._ensure_column_index('_SHOW_PROCEDURES_Col_index')

                for cur_procedure_rs in show_procedures_rs:
                    procedure_name = cur_procedure_rs[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_procedure_name]]
                    argument_list = cur_procedure_rs[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_argument_list]]
                    sql, args_list = self.create_parameterized_query_string(
                        argument_list,
                        self._sql_show_parameters_procedure,
                        column_name_pattern
                    )
                    rs: tuple = self.call_show_metadata(sql, [cur_catalog, cur_schema, procedure_name] + args_list, column_name_pattern)

                    # Create Column name / Column Index mapping for SHOW PARAMETERS
                    self._ensure_column_index('_SHOW_PARAMETERS_PRO_Col_index')

                    # Append specific name at the end of result set since SHOW PARAMETERS doesn't have required column to render specific name
                    # Therefore we need to retrieve specific name here and pass into post-processor with intermediate result set
                    if len(rs) != 0:
                        if 'specific_name' not in self._cursor._SHOW_PARAMETERS_PRO_Col_index:
                            self._cursor._SHOW_PARAMETERS_PRO_Col_index['specific_name'] = len(self._cursor._SHOW_PARAMETERS_PRO_Col_index)
                        specific_name = self.get_specific_name(procedure_name, argument_list)
                        for row in rs:
                            row.append(specific_name)
                    intermediate_rs.append(rs)


        _logger.debug("Successfully executed SHOW PARAMETERS for catalog = %s, schema = %s, procedure_name_pattern = %s, columnNamePattern = %s", catalog, schema_pattern, procedure_name_pattern, column_name_pattern)

        return intermediate_rs

    def get_functions(self, catalog: str = None, schema_pattern: str = None, function_name_pattern: str = None,
                      is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_functions to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        function_name_pattern : Optional[str] A valid pattern for desired function names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for SHOW FUNCTIONS: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                sql = self._sql_show_functions if self.is_none_or_empty(function_name_pattern) else self._sql_show_functions_like
                intermediate_rs.append(self.call_show_metadata(sql, [cur_catalog, cur_schema], function_name_pattern))

                # Create Column name / Column Index mapping for SHOW FUNCTIONS
                self._ensure_column_index('_SHOW_FUNCTIONS_Col_index')

        _logger.debug("Successfully executed SHOW FUNCTIONS for catalog = %s, schema_pattern = %s, function_name_pattern = %s", catalog, schema_pattern, function_name_pattern)

        return intermediate_rs

    def get_function_columns(self, catalog: str = None, schema_pattern: str = None, function_name_pattern: str = None,
                              column_name_pattern: str = None, is_single_database_metadata: bool = True) -> typing.List[typing.Tuple]:
        """
        Helper function for metadata API get_function_columns to return intermediate result for post-processing

        Parameters
        ----------
        catalog : Optional[str] The name of the catalog
        schema_pattern : Optional[str] A valid pattern for desired schemas
        function_name_pattern : Optional[str] A valid pattern for desired function names
        column_name_pattern : Optional[str] A valid pattern for desired column names
        is_single_database_metadata : Optional[bool] Whether or not to only return current connected database metadata

        Returns
        -------
        A list containing several result set for function columns: list
        """
        intermediate_rs: typing.List[typing.Tuple[typing.Any, ...]] = []

        # Get catalog list
        catalog_list: typing.List = self.get_catalog_list(catalog, is_single_database_metadata)

        for cur_catalog in catalog_list:
            # Get schema list
            schema_list: typing.List = self.get_schema_list(cur_catalog, schema_pattern)

            for cur_schema in schema_list:
                # Get function list
                sql = self._sql_show_functions if self.is_none_or_empty(function_name_pattern) else self._sql_show_functions_like
                show_functions_rs = self.call_show_metadata(sql, [cur_catalog, cur_schema], function_name_pattern)

                # Create Column name / Column Index mapping for SHOW FUNCTIONS
                self._ensure_column_index('_SHOW_FUNCTIONS_Col_index')

                for cur_function_rs in show_functions_rs:
                    function_name = cur_function_rs[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_function_name]]
                    argument_list = cur_function_rs[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_argument_list]]
                    sql, args_list = self.create_parameterized_query_string(
                        argument_list,
                        self._sql_show_parameters_function,
                        column_name_pattern
                    )
                    rs: tuple = self.call_show_metadata(sql, [cur_catalog, cur_schema, function_name]  + args_list, column_name_pattern)

                    # Create Column name / Column Index mapping for SHOW PARAMETERS
                    self._ensure_column_index('_SHOW_PARAMETERS_FUNC_Col_index')

                    # Append specific name at the end of result set since SHOW PARAMETERS doesn't have required column to render specific name
                    # Therefore we need to retrieve specific name here and pass into post-processor with intermediate result set
                    if len(rs) != 0:
                        if 'specific_name' not in self._cursor._SHOW_PARAMETERS_FUNC_Col_index:
                            self._cursor._SHOW_PARAMETERS_FUNC_Col_index['specific_name'] = len(self._cursor._SHOW_PARAMETERS_FUNC_Col_index)
                        specific_name = self.get_specific_name(function_name, argument_list)
                        for row in rs:
                            row.append(specific_name)
                    intermediate_rs.append(rs)

        _logger.debug(
            "Successfully executed SHOW PARAMETERS for catalog = %s, schema = %s, function_name_pattern = %s, columnNamePattern = %s",
            catalog, schema_pattern, function_name_pattern, column_name_pattern)

        return intermediate_rs


    def get_catalog_list(self, catalog: str, is_single_database_metadata: bool) -> typing.List:
        """
        Helper function to get a list of catalog name from SHOW DATABASES

        Parameters
        ----------
        catalog : The name of the catalog
        is_single_database_metadata : Whether to only return current connected database metadata

        Returns
        -------
        A list of catalog from SHOW DATABASES: list
        """
        cur_catalog = self._cursor.cur_catalog()

        if is_single_database_metadata:
            return [cur_catalog] if self.is_none_or_empty(catalog) or cur_catalog == catalog else []

        catalog_rs = self.call_show_databases()
        self._ensure_column_index('_SHOW_DATABASES_Col_index')
        db_name_idx = self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]

        if self.is_none_or_empty(catalog):
            return [row[db_name_idx] for row in catalog_rs]

        for row in catalog_rs:
            if row[db_name_idx] == catalog:
                return [catalog]

        return []

    def get_schema_list(self, catalog: str, schema: str = None) -> typing.List:
        """
        Helper function to get a list of schema name from SHOW SCHEMAS

        Parameters
        ----------
        catalog : The name of the catalog
        schema : The schema name / pattern

        Returns
        -------
        A list of schema from SHOW SCHEMAS: list
        """
        sql = self._sql_show_schemas if self.is_none_or_empty(schema) else self._sql_show_schemas_like
        show_schemas_rs: typing.Tuple = self.call_show_metadata(sql, [catalog], schema)

        # Create Column name / Column Index mapping for SHOW SCHEMAS
        self._ensure_column_index('_SHOW_SCHEMAS_Col_index')

        schema_list: typing.List = []
        for schema_rs in show_schemas_rs:
            schema_list.append(schema_rs[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]])
        return schema_list

    def get_table_list(self, catalog: str, schema: str, table: str = None) -> typing.List:
        """
        Helper function to get a list of table name from SHOW TABLES

        Parameters
        ----------
        catalog : The name of the catalog
        schema : The name of the schema
        table : The table name / pattern

        Returns
        -------
        A list of table from SHOW TABLES: list
        """
        sql = self._sql_show_tables if self.is_none_or_empty(table) else self._sql_show_tables_like
        show_tables_rs: typing.Tuple = self.call_show_metadata(sql, [catalog, schema], table)

        # Create Column name / Column Index mapping for SHOW TABLES
        self._ensure_column_index('_SHOW_TABLES_Col_index')

        table_list: typing.List = []
        for table_rs in show_tables_rs:
            table_list.append(table_rs[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]])
        return table_list

    def call_show_databases(self) -> typing.Tuple:
        return self._execute_and_fetch(self._sql_show_databases)

    def call_show_metadata(self, sql: str, params: typing.List[str], pattern: str = None) -> typing.Tuple:
        """Generic helper function to process SHOW command request"""
        if pattern:
            params.append(pattern)
        return self._execute_and_fetch(sql, params)

    def _execute_and_fetch(self, sql: str, params: list = None) -> typing.Tuple:
        """Generic execute and fetch method"""
        _logger.debug("Executing SQL: %s", sql)
        self._cursor.execute(sql, params) if params else self._cursor.execute(sql)
        return self._cursor.fetchall()

    def _ensure_column_index(self, index_attr: str) -> None:
        """Ensures column index mapping exists"""
        if getattr(self._cursor, index_attr) is None:
            setattr(self._cursor, index_attr, self.build_column_name_index_map())

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