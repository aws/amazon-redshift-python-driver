import logging
import typing
import itertools

from redshift_connector.metadataAPIHelper import MetadataAPIHelper

from redshift_connector.config import (
    ClientProtocolVersion,
    DbApiParamstyle,
    _client_encoding,
    table_type_clauses,
)

from redshift_connector.error import (
    InterfaceError,
)

_logger: logging.Logger = logging.getLogger(__name__)


class MetadataAPIPostProcessor(MetadataAPIHelper):
    """
    A class that handles post-processing of metadata API results from Redshift.
    Inherits from MetadataAPIHelper which provides utility functions for metadata operations.

    Workflow:
        1. Sets up row description for result set
        2. Handles empty results case
        3. Flattens input if needed (for list of tuples)
        4. Applies specific transformation to rows
        5. Validates row lengths
        6. Returns final tuple result
    """

    def __init__(self: "MetadataAPIPostProcessor", cursor_instance) -> None:
        super().__init__()
        self._cursor = cursor_instance

    def _generic_post_processing(
            self,
            metadata_api: str,
            intermediate_rs: typing.Union[typing.Tuple, typing.List[typing.Tuple]],
            result_metadata: typing.Any,
            col_index: typing.Optional[typing.Dict],
            transform_rows_func: typing.Callable,
            additional_args: typing.Dict = None,
    ) -> typing.Tuple:
        """
        Generic helper function for post-processing results.

        Args:
            metadata_api: Name of the metadata API being processed
            intermediate_rs: The intermediate result set
            result_metadata: Metadata for the result (column name, data type ...)
            col_index: Column names to indices dictionary
            transform_rows_func: Function to transform rows specific to each post-processing
            additional_args: Optional dictionary of additional arguments for transform_rows_func

        Returns:
            Processed tuple results
        """
        _logger.debug(f"Calling generic post processing for {metadata_api}")

        # Initialize result set metadata
        self.set_row_description(result_metadata.columns)

        # Handle empty results case
        if col_index is None:
            return ()

        # Convert input to iterator of rows, flattening if needed
        rows = (
            itertools.chain.from_iterable(intermediate_rs)
            if isinstance(intermediate_rs, list)
            else intermediate_rs
        )

        # Apply specific transformation to rows
        final_rows = transform_rows_func(rows) if additional_args is None else transform_rows_func(rows, **additional_args)

        # Validate and convert to tuple
        result_tuples: typing.Tuple = tuple(
            self._validate_row_length(row, result_metadata.column_count)
            for row in final_rows
        )

        return result_tuples

    @staticmethod
    def _validate_row_length(row: typing.List[typing.Any], expected_col_count: int):
        """
        Validates that a row has the expected number of columns.
        Raises ValueError if validation fails.
        """
        if len(row) != expected_col_count:
            raise ValueError(f"Row length {len(row)} does not match expected column count {expected_col_count}")
        return row


    def get_catalogs_post_processing(self, intermediate_rs: typing.Tuple) -> typing.Tuple:
        def transform_catalogs_rows(rows):
            return (
                [row[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]]]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_catalogs",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_catalogs_result_metadata,
            col_index=self._cursor._SHOW_DATABASES_Col_index,
            transform_rows_func=transform_catalogs_rows
        )

    def get_schemas_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_schemas_rows(rows):
            return (
                [
                    row[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]],
                    row[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_database_name]]
                ]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_schemas",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_schemas_result_metadata,
            col_index=self._cursor._SHOW_SCHEMAS_Col_index,
            transform_rows_func=transform_schemas_rows
        )

    def get_tables_post_processing(self, intermediate_rs: typing.List[typing.Tuple], types: list) -> typing.Tuple:
        def transform_tables_rows(rows, table_types=None):
            return (
                [
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_database_name]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_schema_name]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_type]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_remarks]],
                    self._empty_string,
                    self._empty_string,
                    self._empty_string,
                    self._empty_string,
                    self._empty_string,
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_owner]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_last_altered_time]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_last_modified_time]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_dist_style]],
                    row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_subtype]],
                ]
                for row in rows
                if table_types is None or len(table_types) == 0 or
                   row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_type]] in table_types
            )

        return self._generic_post_processing(
            metadata_api="get_tables",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_tables_result_metadata,
            col_index=self._cursor._SHOW_TABLES_Col_index,
            transform_rows_func=transform_tables_rows,
            additional_args={'table_types': types}
        )

    def get_columns_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_columns_rows(rows):
            return (
                [
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_database_name]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_schema_name]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_table_name]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_name]],
                    sql_type,
                    rs_type,
                    self.get_column_size(
                        rs_type,
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_precision]],
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_character_maximum_length]]
                    ),
                    None,
                    self.get_decimal_digits(
                        rs_type,
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_scale]],
                        precisions,
                        date_time_customize_precision
                    ),
                    self.get_num_prefix_radix(rs_type),
                    self.get_nullable(row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_is_nullable]]),
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_remarks]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_default]],
                    sql_type,
                    None,
                    self.get_column_size(
                        rs_type,
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_precision]],
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_character_maximum_length]]
                    ),
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_ordinal_position]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_is_nullable]],
                    None,
                    None,
                    None,
                    None,
                    self.get_auto_increment(
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_default]]),
                    self.get_auto_increment(
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_default]]),
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_sort_key_type]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_sort_key]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_dist_key]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_encoding]],
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_collation]]
                ]
                for row in rows
                for rs_type, date_time_customize_precision, precisions in [self.get_second_fraction(
                row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_data_type]]
            )]
                for sql_type in [self.get_sql_type(rs_type)]
            )

        return self._generic_post_processing(
            metadata_api="get_columns",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_columns_result_metadata,
            col_index=self._cursor._SHOW_COLUMNS_Col_index,
            transform_rows_func=transform_columns_rows
        )

    def get_primary_keys_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_primary_keys_rows(rows):
            return (
                [
                    row[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_database_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_schema_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_table_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_column_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_key_seq]],
                    row[self._cursor._SHOW_CONSTRAINTS_PK_Col_index[self._SHOW_CONSTRAINTS_PK_pk_name]]
                ]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_primary_keys",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_primary_keys_result_metadata,
            col_index=self._cursor._SHOW_CONSTRAINTS_PK_Col_index,
            transform_rows_func=transform_primary_keys_rows
        )

    def get_foreign_keys_post_processing(self, intermediate_rs: typing.List[typing.Tuple], imported: bool) -> typing.Tuple:
        def transform_foreign_keys_rows(rows):
            return (
                [
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_pk_database_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_pk_schema_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_pk_table_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_pk_column_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_fk_database_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_fk_schema_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_fk_table_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_fk_column_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_key_seq]],
                    (self._imported_key_no_action
                     if row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_update_rule]] is None
                     else row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_update_rule]]),
                    (self._imported_key_no_action
                     if row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_delete_rule]] is None
                     else row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_delete_rule]]),
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_fk_name]],
                    row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_pk_name]],
                    (self._imported_key_not_deferrable
                     if row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[
                        self._SHOW_CONSTRAINTS_FK_deferrability]] is None
                     else row[self._cursor._SHOW_CONSTRAINTS_FK_Col_index[self._SHOW_CONSTRAINTS_FK_deferrability]])
                ]
                for row in rows
            )

        post_processed_rs: typing.Tuple = self._generic_post_processing(
            metadata_api="get_foreign_keys",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_foreign_keys_result_metadata,
            col_index=self._cursor._SHOW_CONSTRAINTS_FK_Col_index,
            transform_rows_func=transform_foreign_keys_rows
        )

        if imported:
            return tuple(sorted(post_processed_rs, key=lambda x: (x[self._get_imported_key_pk_catalog_index], x[self._get_imported_key_pk_schema_index], x[self._get_imported_key_pk_table_index], x[self._get_imported_key_key_seq_index])))
        else:
            return tuple(sorted(post_processed_rs, key=lambda x: (x[self._get_exported_key_fk_catalog_index], x[self._get_exported_key_fk_schema_index], x[self._get_exported_key_fk_table_index], x[self._get_exported_key_key_seq_index])))

    def get_best_row_identifier_post_processing(self, intermediate_rs: typing.List[typing.Tuple], scope: int) -> typing.Tuple:
        def transform_best_row_identifier_rows(rows, cur_scope):
            return (
                [
                    cur_scope,
                    row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_name]],
                    sql_type,
                    rs_type,
                    self.get_column_size(
                        rs_type,
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_precision]],
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_character_maximum_length]]
                    ),
                    None,
                    self.get_decimal_digits(
                        rs_type,
                        row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_scale]],
                        precisions,
                        date_time_customize_precision
                    ),
                    1
                ]
                for row in rows
                for rs_type, date_time_customize_precision, precisions in [
                self.get_second_fraction(row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_data_type]])
            ]
                for sql_type in [self.get_sql_type(rs_type)]
            )

        return self._generic_post_processing(
            metadata_api="get_best_row_identifier",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_best_row_identifier_result_metadata,
            col_index=self._cursor._SHOW_COLUMNS_Col_index,
            transform_rows_func=transform_best_row_identifier_rows,
            additional_args={'cur_scope': scope}
        )

    def get_column_privileges_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_column_privileges_rows(rows):
            return (
                [
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_database_name]],
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_schema_name]],
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_table_name]],
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_column_name]],
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_grantor_name]],
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_identity_name]],
                    row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_privilege_type]],
                    self.get_is_grantable(row[self._cursor._SHOW_GRANTS_COLUMN_Col_index[self._SHOW_GRANT_admin_option]])
                ]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_column_privileges",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_column_privileges_result_metadata,
            col_index=self._cursor._SHOW_GRANTS_COLUMN_Col_index,
            transform_rows_func=transform_column_privileges_rows
        )

    def get_table_privileges_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_table_privileges_rows(rows):
            return (
                [
                    row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_database_name]],
                    row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_schema_name]],
                    row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_object_name]],
                    row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_grantor_name]],
                    row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_identity_name]],
                    row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_privilege_type]],
                    self.get_is_grantable(row[self._cursor._SHOW_GRANTS_TABLE_Col_index[self._SHOW_GRANT_admin_option]])
                ]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_table_privileges",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_table_privileges_result_metadata,
            col_index=self._cursor._SHOW_GRANTS_TABLE_Col_index,
            transform_rows_func=transform_table_privileges_rows
        )


    def get_procedures_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_procedures_rows(rows):
            """
            Transforms intermediate result set to match JDBC DatabaseMetaData.getProcedures() specification.

            The transformed rows contain the following columns in order:
            1. PROCEDURE_CAT (catalog/database name)
            2. PROCEDURE_SCHEM (schema name)
            3. PROCEDURE_NAME (procedure name)
            4. Reserved for future use
            5. Reserved for future use
            6. Reserved for future use
            7. REMARKS (procedure description, empty string in this case since Redshift doesn't support it)
            8. PROCEDURE_TYPE (return type)
            9. SPECIFIC_NAME (unique identifier for procedure)

            Args:
                rows: Original procedure metadata rows from SHOW PROCEDURES command

            Returns:
                Generator yielding transformed rows matching JDBC getProcedures spec
            """
            return (
                [
                    row[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_database_name]],
                    row[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_schema_name]],
                    row[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_procedure_name]],
                    None,
                    None,
                    None,
                    self._empty_string,
                    self.get_procedure_type(row[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_return_type]]),
                    self.get_specific_name(
                        row[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_procedure_name]],
                        row[self._cursor._SHOW_PROCEDURES_Col_index[self._SHOW_PROCEDURES_argument_list]])
                ]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_procedures",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_procedures_result_metadata,
            col_index=self._cursor._SHOW_PROCEDURES_Col_index,
            transform_rows_func=transform_procedures_rows
        )

    def get_procedure_columns_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_procedure_columns_rows(rows):
            """
            Transforms intermediate result set to match JDBC DatabaseMetaData.getProcedureColumns() specification.

            The transformed rows contain the following columns in order:
            1. PROCEDURE_CAT (catalog/database name)
            2. PROCEDURE_SCHEM (schema name)
            3. PROCEDURE_NAME (procedure name)
            4. COLUMN_NAME (parameter name)
            5. COLUMN_TYPE (parameter type)
            6. DATA_TYPE (SQL type)
            7. TYPE_NAME (SQL type name)
            8. PRECISION (column size)
            9. LENGTH (length in bytes)
            10. SCALE (decimal digits)
            11. RADIX (numeric base - typically 10)
            12. NULLABLE (parameter nullability)
            13. REMARKS (parameter description. empty string in this case since Redshift doesn't support it)
            14. COLUMN_DEF (default value)
            15. SQL_DATA_TYPE (unused but return SQL type to prevent breaking change)
            16. SQL_DATETIME_SUB (unused)
            17. CHAR_OCTET_LENGTH (max length for character types)
            18. ORDINAL_POSITION (parameter index)
            19. IS_NULLABLE ("YES"/"NO")
            20. SPECIFIC_NAME (unique identifier for procedure)

            Args:
                rows: Original procedure parameter metadata rows from SHOW PARAMETERS command

            Returns:
                Generator yielding transformed rows matching JDBC getProcedureColumns spec
            """
            return (
                [
                    row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_database_name]],
                    row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_schema_name]],
                    row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_procedure_name]],
                    row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_parameter_name]],
                    self.get_procedure_column_type(row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_parameter_type]]),
                    sql_type,
                    rs_type,
                    self.get_column_size(rs_type,
                                         row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[
                                             self._SHOW_PARAMETERS_numeric_precision]],
                                         row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[
                                             self._SHOW_PARAMETERS_character_maximum_length]]),
                    self.get_column_length(rs_type),
                    self.get_decimal_digits(rs_type,
                                            row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[
                                                self._SHOW_PARAMETERS_numeric_scale]],
                                            precisions,
                                            date_time_customize_precision),
                    self._default_radix,
                    self._procedure_nullable_unknown,
                    self._empty_string,
                    None,
                    sql_type,
                    None,
                    None,
                    row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_ordinal_position]],
                    self._empty_string,
                    row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._specific_name]]
                ]
                for row in rows
                for rs_type, date_time_customize_precision, precisions in [
                self.get_second_fraction(row[self._cursor._SHOW_PARAMETERS_PRO_Col_index[self._SHOW_PARAMETERS_data_type]])
            ]
                for sql_type in [self.get_sql_type(rs_type)]
            )

        return self._generic_post_processing(
            metadata_api="get_procedure_columns",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_procedure_columns_result_metadata,
            col_index=self._cursor._SHOW_PARAMETERS_PRO_Col_index,
            transform_rows_func=transform_procedure_columns_rows
        )


    def get_functions_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_functions_rows(rows):
            """
            Transforms intermediate result set to match JDBC DatabaseMetaData.getFunctions() specification.

            The transformed rows contain the following columns in order:
            1. FUNCTION_CAT (catalog/database name)
            2. FUNCTION_SCHEM (schema name)
            3. FUNCTION_NAME (function name)
            4. REMARKS (function description, empty string in this case since Redshift doesn't support it)
            5. FUNCTION_TYPE (return type)
            6. SPECIFIC_NAME (unique identifier for function)

            Args:
                rows: Original function metadata rows from SHOW FUNCTIONS command

            Returns:
                Generator yielding transformed rows matching JDBC getFunctions spec
            """
            return (
                [
                    row[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_database_name]],
                    row[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_schema_name]],
                    row[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_function_name]],
                    self._empty_string,
                    self.get_function_type(row[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_return_type]]),
                    self.get_specific_name(
                        row[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_function_name]],
                        row[self._cursor._SHOW_FUNCTIONS_Col_index[self._SHOW_FUNCTIONS_argument_list]])
                ]
                for row in rows
            )

        return self._generic_post_processing(
            metadata_api="get_functions",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_functions_result_metadata,
            col_index=self._cursor._SHOW_FUNCTIONS_Col_index,
            transform_rows_func=transform_functions_rows
        )


    def get_function_columns_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_function_columns_rows(rows):
            """
            Transforms intermediate result set to match JDBC DatabaseMetaData.getFunctionColumns() specification.

            The transformed rows contain the following columns in order:
            1. FUNCTION_CAT (catalog/database name)
            2. FUNCTION_SCHEM (schema name)
            3. FUNCTION_NAME (function name)
            4. COLUMN_NAME (parameter name)
            5. COLUMN_TYPE (parameter type)
            6. DATA_TYPE (SQL type)
            7. TYPE_NAME (SQL type name)
            8. PRECISION (column size)
            9. LENGTH (length in bytes)
            10. SCALE (decimal digits)
            11. RADIX (numeric base - typically 10)
            12. NULLABLE (parameter nullability)
            13. REMARKS (parameter description, empty string in this case since Redshift doesn't support it)
            14. CHAR_OCTET_LENGTH (max length for character types)
            15. ORDINAL_POSITION (parameter index)
            16. IS_NULLABLE ("YES"/"NO")
            17. SPECIFIC_NAME (unique identifier for function)

            Args:
                rows: Original function parameter metadata rows from SHOW PARAMETERS command

            Returns:
                Generator yielding transformed rows matching JDBC getFunctionColumns spec
            """
            return (
                [
                    row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_database_name]],
                    row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_schema_name]],
                    row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_function_name]],
                    row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_parameter_name]],
                    self.get_function_column_type(row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_parameter_type]]),
                    sql_type,
                    rs_type,
                    self.get_column_size(rs_type,
                                         row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[
                                             self._SHOW_PARAMETERS_numeric_precision]],
                                         row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[
                                             self._SHOW_PARAMETERS_character_maximum_length]]),
                    self.get_column_length(rs_type),
                    self.get_decimal_digits(rs_type,
                                            row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[
                                                self._SHOW_PARAMETERS_numeric_scale]],
                                            precisions,
                                            date_time_customize_precision),
                    self._default_radix,
                    self._function_nullable_unknown,
                    self._empty_string,
                    None,
                    row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_ordinal_position]],
                    self._empty_string,
                    row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._specific_name]]
                ]
                for row in rows
                for rs_type, date_time_customize_precision, precisions in [
                self.get_second_fraction(row[self._cursor._SHOW_PARAMETERS_FUNC_Col_index[self._SHOW_PARAMETERS_data_type]])
            ]
                for sql_type in [self.get_sql_type(rs_type)]
            )

        return self._generic_post_processing(
            metadata_api="get_function_columns",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_function_columns_result_metadata,
            col_index=self._cursor._SHOW_PARAMETERS_FUNC_Col_index,
            transform_rows_func=transform_function_columns_rows
        )

    def get_table_types_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        def transform_get_table_types_rows(rows):
            """
            Transforms intermediate result set to match JDBC DatabaseMetaData.getTableTypes() specification.

            The transformed rows contain the following columns in order:
            1. TABLE_TYPE

            Args:
                rows: Pre-defined result set which contain fixed table type lise

            Returns:
                Generator yielding transformed rows matching JDBC getTableTypes spec
            """
            return ([row] for row in rows)

        return self._generic_post_processing(
            metadata_api="get_table_types",
            intermediate_rs=intermediate_rs,
            result_metadata=self._get_table_type_result_metadata,
            col_index={"table_type": 0},
            transform_rows_func=transform_get_table_types_rows
        )

    def set_row_description(self, cur_column: typing.Dict) -> None:
        """
        Sets up the row description for the result set.
        Creates a standardized description dictionary for each column.

        Args:
            cur_column: Dictionary mapping column names to their OIDs
        """

        if self._cursor.ps is None:
            self._cursor.ps = {}

        self._cursor.ps["row_desc"] = []

        for col_name, col_oid in cur_column.items():
            row_desc: typing.Dict = {'autoincrement': 0,
                                     'catalog_name': typing.cast(str, self._empty_string).encode(_client_encoding),
                                     'column_attrnum': 0,
                                     'column_name': typing.cast(str, self._empty_string).encode(_client_encoding),
                                     'format': 1,
                                     'func': None,
                                     'label': typing.cast(str, col_name).encode(_client_encoding),
                                     'nullable': 0,
                                     'read_only': 0,
                                     'redshift_connector_fc': 1,
                                     'schema_name': typing.cast(str, self._empty_string).encode(_client_encoding),
                                     'searchable': 1,
                                     'table_name': typing.cast(str, self._empty_string).encode(_client_encoding),
                                     'table_oid': 0,
                                     'type_modifier': 0,
                                     'type_oid': col_oid,
                                     'type_size': -1}

            self._cursor.ps["row_desc"].append(row_desc)
