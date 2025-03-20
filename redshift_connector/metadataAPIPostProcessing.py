import logging
import typing
import re

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


class MetadataAPIPostProcessing(MetadataAPIHelper):
    def __init__(self: "MetadataAPIPostProcessing", cursor_instance) -> None:
        super().__init__()
        self._cursor = cursor_instance


    def get_catalog_post_processing(self, intermediate_rs: typing.Tuple) -> typing.Tuple:
        _logger.debug("Calling get_catalog_post_processing")
        final_rs: typing.Tuple = ()

        self.set_row_description(self._get_catalogs_col)

        if self._cursor._SHOW_DATABASES_Col_index is not None:
            for row in intermediate_rs:
                final_row: typing.List = [row[self._cursor._SHOW_DATABASES_Col_index[self._SHOW_DATABASES_database_name]]]

                if len(final_row) != self._CatalogsColNum:
                    raise InterfaceError("Tuple row has incorrect column number")

                final_rs += (final_row,)

        return final_rs

    def get_schema_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        _logger.debug("Calling get_schema_post_processing")
        final_rs: typing.Tuple = ()

        self.set_row_description(self._get_schemas_col)

        if self._cursor._SHOW_SCHEMAS_Col_index is not None:
            for rs in intermediate_rs:
                for row in rs:
                    final_row: typing.List = [row[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_schema_name]],
                                          row[self._cursor._SHOW_SCHEMAS_Col_index[self._SHOW_SCHEMA_database_name]]]

                    if len(final_row) != self._SchemasColNum:
                        raise InterfaceError("Tuple row has incorrect column number")

                    final_rs += (final_row,)
        return final_rs

    def get_table_post_processing(self, intermediate_rs: typing.List[typing.Tuple], types: list) -> typing.Tuple:
        _logger.debug("Calling get_table_post_processing")
        final_rs: typing.Tuple = ()

        self.set_row_description(self._get_tables_col)

        if self._cursor._SHOW_TABLES_Col_index is not None:
            for rs in intermediate_rs:
                for row in rs:
                    if types is None or len(types) == 0 or row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_type]] in types:
                        final_row: typing.List = [row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_database_name]],
                                          row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_schema_name]],
                                          row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_name]],
                                          row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_table_type]],
                                          row[self._cursor._SHOW_TABLES_Col_index[self._SHOW_TABLES_remarks]],
                                          "", "", "", "", ""]

                        if len(final_row) != self._TablesColNum:
                            raise InterfaceError("Tuple row has incorrect column number")

                        final_rs += (final_row,)
        return final_rs

    def get_column_post_processing(self, intermediate_rs: typing.List[typing.Tuple]) -> typing.Tuple:
        _logger.debug("Calling get_column_post_processing")
        final_rs: typing.Tuple = ()

        self.set_row_description(self._get_columns_col)

        if self._cursor._SHOW_COLUMNS_Col_index is not None:
            for rs in intermediate_rs:
                for row in rs:
                    data_type: str = row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_data_type]]

                    rs_type: str
                    date_time_customize_precision: bool
                    precisions: int

                    rs_type, date_time_customize_precision, precisions = self.get_second_fraction(data_type)

                    sql_type: int = self.get_sql_type(rs_type)

                    final_row: typing.List = [row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_database_name]],
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_schema_name]],
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_table_name]],
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_name]],
                                          sql_type,
                                          rs_type,
                                          self.get_column_size(rs_type, row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_precision]], row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_character_maximum_length]]),
                                          None,
                                          self.get_decimal_digits(rs_type, row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_scale]], precisions, date_time_customize_precision),
                                          self.get_num_prefix_radix(rs_type),
                                          self.get_nullable(row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_is_nullable]]),
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_remarks]],
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_default]],
                                          sql_type,
                                          None,
                                          self.get_column_size(rs_type, row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_numeric_precision]], row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_character_maximum_length]]),
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_ordinal_position]],
                                          row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_is_nullable]],
                                          None,
                                          None,
                                          None,
                                          None,
                                          self.get_auto_increment(row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_default]]),
                                          self.get_auto_increment(row[self._cursor._SHOW_COLUMNS_Col_index[self._SHOW_COLUMNS_column_default]])
                                          ]
                    if len(final_row) != self._ColumnsColNum:
                        raise InterfaceError("Tuple row has incorrect column number")

                    final_rs += (final_row,)

        return final_rs

    def set_row_description(self, cur_column: typing.Dict) -> None:
        self._cursor.ps["row_desc"] = []

        for col_name, col_oid in cur_column.items():
            row_desc: typing.Dict = {'autoincrement': 0,
                                     'catalog_name': typing.cast(str, "").encode(_client_encoding),
                                     'column_attrnum': 0,
                                     'column_name': typing.cast(str, "").encode(_client_encoding),
                                     'format': 1,
                                     'func': None,
                                     'label': typing.cast(str, col_name).encode(_client_encoding),
                                     'nullable': 0,
                                     'read_only': 0,
                                     'redshift_connector_fc': 1,
                                     'schema_name': typing.cast(str, "").encode(_client_encoding),
                                     'searchable': 1,
                                     'table_name': typing.cast(str, "").encode(_client_encoding),
                                     'table_oid': 0,
                                     'type_modifier': 0,
                                     'type_oid': col_oid,
                                     'type_size': -1}

            self._cursor.ps["row_desc"].append(row_desc)

