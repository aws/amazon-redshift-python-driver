import typing
from collections import deque
from itertools import count, islice
from typing import TYPE_CHECKING
from warnings import warn

import numpy  # type: ignore
import pandas  # type: ignore

import redshift_connector
from redshift_connector.config import table_type_clauses
from redshift_connector.error import InterfaceError, ProgrammingError

if TYPE_CHECKING:
    from redshift_connector.core import Connection


class Cursor:
    """A cursor object is returned by the :meth:`~Connection.cursor` method of
    a connection. It has the following attributes and methods:

    .. attribute:: arraysize

        This read/write attribute specifies the number of rows to fetch at a
        time with :meth:`fetchmany`.  It defaults to 1.

    .. attribute:: connection

        This read-only attribute contains a reference to the connection object
        (an instance of :class:`Connection`) on which the cursor was
        created.

        This attribute is part of a DBAPI 2.0 extension.  Accessing this
        attribute will generate the following warning: ``DB-API extension
        cursor.connection used``.

    .. attribute:: rowcount

        This read-only attribute contains the number of rows that the last
        ``execute()`` or ``executemany()`` method produced (for query
        statements like ``SELECT``) or affected (for modification statements
        like ``UPDATE``).

        The value is -1 if:

        - No ``execute()`` or ``executemany()`` method has been performed yet
          on the cursor.
        - There was no rowcount associated with the last ``execute()``.
        - At least one of the statements executed as part of an
          ``executemany()`` had no row count associated with it.
        - Using a ``SELECT`` query statement on PostgreSQL server older than
          version 9.
        - Using a ``COPY`` query statement on PostgreSQL server version 8.1 or
          older.

        This attribute is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. attribute:: description

        This read-only attribute is a sequence of 7-item sequences.  Each value
        contains information describing one result column.  The 7 items
        returned for each column are (name, type_code, display_size,
        internal_size, precision, scale, null_ok).  Only the first two values
        are provided by the current implementation.

        This attribute is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self: "Cursor", connection: "Connection", paramstyle=None) -> None:
        self._c: typing.Optional["Connection"] = connection
        self.arraysize: int = 1
        self.ps: typing.Optional[typing.Dict[str, typing.Any]] = None
        self._row_count: int = -1
        self._cached_rows: deque = deque()
        if paramstyle is None:
            self.paramstyle: str = redshift_connector.paramstyle
        else:
            self.paramstyle = paramstyle

    def __enter__(self: "Cursor") -> "Cursor":
        return self

    def __exit__(self: "Cursor", exc_type, exc_value, traceback) -> None:
        self.close()

    @property
    def connection(self: "Cursor") -> typing.Optional["Connection"]:
        warn("DB-API extension cursor.connection used", stacklevel=3)
        return self._c

    @property
    def rowcount(self: "Cursor") -> int:
        return self._row_count

    description = property(lambda self: self._getDescription())

    def _getDescription(self: "Cursor") -> typing.Optional[typing.List[typing.Optional[typing.Tuple]]]:
        if self.ps is None:
            return None
        row_desc = self.ps["row_desc"]
        if len(row_desc) == 0:
            return None
        columns: typing.List[typing.Optional[typing.Tuple]] = []
        for col in row_desc:
            columns.append((col["name"], col["type_oid"], None, None, None, None, None))
        return columns

    ##
    # Executes a database operation.  Parameters may be provided as a sequence
    # or mapping and will be bound to variables in the operation.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    def execute(self: "Cursor", operation, args=None, stream=None, merge_socket_read=False) -> "Cursor":
        """Executes a database operation.  Parameters may be provided as a
        sequence, or as a mapping, depending upon the value of
        :data:`paramstyle`.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute.

        :param args:
            If :data:`paramstyle` is ``qmark``, ``numeric``, or ``format``,
            this argument should be an array of parameters to bind into the
            statement.  If :data:`paramstyle` is ``named``, the argument should
            be a dict mapping of parameters.  If the :data:`paramstyle` is
            ``pyformat``, the argument value may be either an array or a
            mapping.

        :param stream: This is a extension for use with the PostgreSQL
            `COPY
            <http://www.postgresql.org/docs/current/static/sql-copy.html>`_
            command. For a COPY FROM the parameter must be a readable file-like
            object, and for COPY TO it must be writable.

            .. versionadded:: 1.9.11
        """
        if self._c is None:
            raise InterfaceError("Cursor closed")
        if self._c._sock is None:
            raise InterfaceError("connection is closed")

        try:
            self.stream = stream
            # For Redshift, we need to begin transaction and then to process query
            # In the end we can use commit or rollback to end the transaction
            if not self._c.in_transaction and not self._c.autocommit:
                self._c.execute(self, "begin transaction", None)
            self._c.merge_socket_read = merge_socket_read
            self._c.execute(self, operation, args)
        except AttributeError as e:
            raise e
        return self

    def executemany(self: "Cursor", operation, param_sets) -> "Cursor":
        """Prepare a database operation, and then execute it against all
        parameter sequences or mappings provided.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute
        :param parameter_sets:
            A sequence of parameters to execute the statement with. The values
            in the sequence should be sequences or mappings of parameters, the
            same as the args argument of the :meth:`execute` method.
        """
        rowcounts: typing.List[int] = []
        for parameters in param_sets:
            self.execute(operation, parameters)
            rowcounts.append(self._row_count)

        self._row_count = -1 if -1 in rowcounts else sum(rowcounts)
        return self

    def fetchone(self: "Cursor") -> typing.Optional["Cursor"]:
        """Fetch the next row of a query result set.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:
            A row as a sequence of field values, or ``None`` if no more rows
            are available.
        """
        try:
            return typing.cast("Cursor", next(self))
        except StopIteration:
            return None
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")
        except AttributeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchmany(self: "Cursor", num: typing.Optional[int] = None) -> typing.Tuple:
        """Fetches the next set of rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param num:

            The number of rows to fetch when called.  If not provided, the
            :attr:`arraysize` attribute value is used instead.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.  If no more rows are available, an empty sequence
            will be returned.
        """
        try:
            return tuple(islice(self, self.arraysize if num is None else num))
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchall(self: "Cursor") -> typing.Tuple:
        """Fetches all remaining rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.
        """
        try:
            return tuple(self)
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def close(self: "Cursor") -> None:
        """Closes the cursor.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._c = None

    def __iter__(self: "Cursor") -> "Cursor":
        """A cursor object is iterable to retrieve the rows from a query.

        This is a DBAPI 2.0 extension.
        """
        return self

    def setinputsizes(self: "Cursor", sizes):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented.
        """
        pass

    def setoutputsize(self: "Cursor", size, column=None):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented.
        """
        pass

    def __next__(self: "Cursor"):
        try:
            return self._cached_rows.popleft()
        except IndexError:
            if self.ps is None:
                raise ProgrammingError("A query hasn't been issued.")
            elif len(self.ps["row_desc"]) == 0:
                raise ProgrammingError("no result set")
            else:
                raise StopIteration()

    def fetch_dataframe(self: "Cursor", num: typing.Optional[int] = None) -> typing.Optional[pandas.DataFrame]:
        """Return a dataframe of the last query results."""
        try:
            columns: typing.List = [column[0].decode().lower() for column in self.description]
        except:
            columns = [column[0].lower() for column in self.description]

        if num:
            fetcheddata: tuple = self.fetchmany(num)
        else:
            fetcheddata = self.fetchall()

        result: typing.List = [tuple(column for column in rows) for rows in fetcheddata]
        if len(result) == 0:
            return None
        return pandas.DataFrame(result, columns=columns)

    def write_dataframe(self: "Cursor", df: pandas.DataFrame, table: str) -> None:
        """write same structure dataframe into Redshift database"""
        arrays: numpy.ndarray = df.values
        placeholder: str = ", ".join(["%s"] * len(arrays[0]))
        sql: str = "insert into {table} values ({placeholder})".format(table=table, placeholder=placeholder)
        if len(arrays) == 1:
            self.execute(sql, arrays[0])
        elif len(arrays) > 1:
            self.executemany(sql, arrays)

    def fetch_numpy_array(self: "Cursor", num: typing.Optional[int] = None) -> numpy.ndarray:
        """Return a numpy array of the last query results."""
        if num:
            fetched: typing.Tuple = self.fetchmany(num)
        else:
            fetched = self.fetchall()

        return numpy.array(fetched)

    def get_procedures(
        self: "Cursor",
        catalog: typing.Optional[str] = None,
        schema_pattern: typing.Optional[str] = None,
        procedure_name_pattern: typing.Optional[str] = None,
    ) -> tuple:
        sql: str = (
            "SELECT current_database() AS PROCEDURE_CAT, n.nspname AS PROCEDURE_SCHEM, p.proname AS PROCEDURE_NAME, "
            "NULL, NULL, NULL, d.description AS REMARKS, "
            " CASE p.prokind "
            " WHEN 'f' THEN 2 "
            " WHEN 'p' THEN 1 "
            " ELSE 0 "
            " END AS PROCEDURE_TYPE, "
            " p.proname || '_' || p.prooid AS SPECIFIC_NAME "
            " FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc_info p "
            " LEFT JOIN pg_catalog.pg_description d ON (p.prooid=d.objoid) "
            " LEFT JOIN pg_catalog.pg_class c ON (d.classoid=c.oid AND c.relname='pg_proc') "
            " LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace=pn.oid AND pn.nspname='pg_catalog') "
            " WHERE p.pronamespace=n.oid "
        )
        if schema_pattern is not None:
            sql += " AND n.nspname LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        else:
            sql += "and pg_function_is_visible(p.prooid)"

        if procedure_name_pattern is not None:
            sql += " AND p.proname LIKE {procedure}".format(procedure=self.__escape_quotes(procedure_name_pattern))
        sql += " ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, p.prooid::text "

        self.execute(sql)
        procedures: tuple = self.fetchall()
        return procedures

    def get_schemas(
        self: "Cursor", catalog: typing.Optional[str] = None, schema_pattern: typing.Optional[str] = None
    ) -> tuple:
        sql: str = (
            "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace "
            " WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_' "
            " OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_' "
            " OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) "
        )
        if schema_pattern is not None:
            sql += " AND nspname LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        sql += " ORDER BY TABLE_SCHEM"

        self.execute(sql)
        schemas: tuple = self.fetchall()
        return schemas

    def get_primary_keys(
        self: "Cursor",
        catalog: typing.Optional[str] = None,
        schema: typing.Optional[str] = None,
        table: typing.Optional[str] = None,
    ) -> tuple:
        sql: str = (
            "SELECT "
            "current_database() AS TABLE_CAT, "
            "n.nspname AS TABLE_SCHEM,  "
            "ct.relname AS TABLE_NAME,   "
            "a.attname AS COLUMN_NAME,   "
            "a.attnum AS KEY_SEQ,   "
            "ci.relname AS PK_NAME   "
            "FROM  "
            "pg_catalog.pg_namespace n,  "
            "pg_catalog.pg_class ct,  "
            "pg_catalog.pg_class ci, "
            "pg_catalog.pg_attribute a, "
            "pg_catalog.pg_index i "
            "WHERE "
            "ct.oid=i.indrelid AND "
            "ci.oid=i.indexrelid  AND "
            "a.attrelid=ci.oid AND "
            "i.indisprimary  AND "
            "ct.relnamespace = n.oid "
        )
        if schema is not None:
            sql += " AND n.nspname = {schema}".format(schema=self.__escape_quotes(schema))
        if table is not None:
            sql += " AND ct.relname = {table}".format(table=self.__escape_quotes(table))

        sql += " ORDER BY table_name, pk_name, key_seq"
        self.execute(sql)
        keys: tuple = self.fetchall()
        return keys

    def get_tables(
        self: "Cursor",
        catalog: typing.Optional[str] = None,
        schema_pattern: typing.Optional[str] = None,
        table_name_pattern: typing.Optional[str] = None,
        types: list = [],
    ) -> tuple:
        """Returns the unique public tables which are user-defined within the system"""
        sql: str = ""
        schema_pattern_type: str = self.__schema_pattern_match(schema_pattern)
        if schema_pattern_type == "LOCAL_SCHEMA_QUERY":
            sql = self.__build_local_schema_tables_query(catalog, schema_pattern, table_name_pattern, types)
        elif schema_pattern_type == "NO_SCHEMA_UNIVERSAL_QUERY":
            sql = self.__build_universal_schema_tables_query(catalog, schema_pattern, table_name_pattern, types)
        elif schema_pattern_type == "EXTERNAL_SCHEMA_QUERY":
            sql = self.__build_external_schema_tables_query(catalog, schema_pattern, table_name_pattern, types)

        self.execute(sql)
        tables: tuple = self.fetchall()
        return tables

    def __build_local_schema_tables_query(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        table_name_pattern: typing.Optional[str],
        types: list,
    ) -> str:
        sql: str = (
            "SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, "
            " CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' "
            " WHEN true THEN CASE "
            " WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind "
            "  WHEN 'r' THEN 'SYSTEM TABLE' "
            "  WHEN 'v' THEN 'SYSTEM VIEW' "
            "  WHEN 'i' THEN 'SYSTEM INDEX' "
            "  ELSE NULL "
            "  END "
            " WHEN n.nspname = 'pg_toast' THEN CASE c.relkind "
            "  WHEN 'r' THEN 'SYSTEM TOAST TABLE' "
            "  WHEN 'i' THEN 'SYSTEM TOAST INDEX' "
            "  ELSE NULL "
            "  END "
            " ELSE CASE c.relkind "
            "  WHEN 'r' THEN 'TEMPORARY TABLE' "
            "  WHEN 'p' THEN 'TEMPORARY TABLE' "
            "  WHEN 'i' THEN 'TEMPORARY INDEX' "
            "  WHEN 'S' THEN 'TEMPORARY SEQUENCE' "
            "  WHEN 'v' THEN 'TEMPORARY VIEW' "
            "  ELSE NULL "
            "  END "
            " END "
            " WHEN false THEN CASE c.relkind "
            " WHEN 'r' THEN 'TABLE' "
            " WHEN 'p' THEN 'PARTITIONED TABLE' "
            " WHEN 'i' THEN 'INDEX' "
            " WHEN 'S' THEN 'SEQUENCE' "
            " WHEN 'v' THEN 'VIEW' "
            " WHEN 'c' THEN 'TYPE' "
            " WHEN 'f' THEN 'FOREIGN TABLE' "
            " WHEN 'm' THEN 'MATERIALIZED VIEW' "
            " ELSE NULL "
            " END "
            " ELSE NULL "
            " END "
            " AS TABLE_TYPE, d.description AS REMARKS, "
            " '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, "
            "'' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION "
            " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c "
            " LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) "
            " LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class') "
            " LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog') "
            " WHERE c.relnamespace = n.oid "
        )
        filter_clause: str = self.__get_table_filter_clause(
            catalog, schema_pattern, table_name_pattern, types, "LOCAL_SCHEMA_QUERY"
        )
        orderby: str = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME "

        return sql + filter_clause + orderby

    def __get_table_filter_clause(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        table_name_pattern: typing.Optional[str],
        types: list,
        schema_pattern_type: str,
    ) -> str:
        filter_clause: str = ""
        use_schemas: str = "SCHEMAS"
        if schema_pattern is not None:
            filter_clause += " AND TABLE_SCHEM LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        if table_name_pattern is not None:
            filter_clause += " AND TABLE_NAME LIKE {table}".format(table=self.__escape_quotes(table_name_pattern))
        if len(types) > 0:
            if schema_pattern_type == "LOCAL_SCHEMA_QUERY":
                filter_clause += " AND (false "
                orclause: str = ""
                for type in types:
                    clauses = table_type_clauses[type]
                    if len(clauses) > 0:
                        cluase = clauses[use_schemas]
                        orclause += " OR ( {cluase} ) ".format(cluase=cluase)
                filter_clause += orclause + ") "

            elif schema_pattern_type == "NO_SCHEMA_UNIVERSAL_QUERY" or schema_pattern_type == "EXTERNAL_SCHEMA_QUERY":
                filter_clause += " AND TABLE_TYPE IN ( "
                length = len(types)
                for type in types:
                    filter_clause += self.__escape_quotes(type)
                    length -= 1
                    if length > 0:
                        filter_clause += ", "
                filter_clause += ") "

        return filter_clause

    def __build_universal_schema_tables_query(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        table_name_pattern: typing.Optional[str],
        types: list,
    ) -> str:
        sql: str = (
            "SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT,"
            " table_schema AS TABLE_SCHEM,"
            " table_name AS TABLE_NAME,"
            " CAST("
            " CASE table_type"
            " WHEN 'BASE TABLE' THEN CASE"
            " WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM TABLE'"
            " WHEN table_schema = 'pg_toast' THEN 'SYSTEM TOAST TABLE'"
            " WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY TABLE'"
            " ELSE 'TABLE'"
            " END"
            " WHEN 'VIEW' THEN CASE"
            " WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM VIEW'"
            " WHEN table_schema = 'pg_toast' THEN NULL"
            " WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY VIEW'"
            " ELSE 'VIEW'"
            " END"
            " WHEN 'EXTERNAL TABLE' THEN 'EXTERNAL TABLE'"
            " END"
            " AS VARCHAR(124)) AS TABLE_TYPE,"
            " REMARKS,"
            " '' as TYPE_CAT,"
            " '' as TYPE_SCHEM,"
            " '' as TYPE_NAME, "
            " '' AS SELF_REFERENCING_COL_NAME,"
            " '' AS REF_GENERATION "
            " FROM svv_tables)"
            " WHERE true "
        )
        filter_clause: str = self.__get_table_filter_clause(
            catalog, schema_pattern, table_name_pattern, types, "NO_SCHEMA_UNIVERSAL_QUERY"
        )
        orderby: str = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME "
        sql += filter_clause + orderby
        return sql

    def __build_external_schema_tables_query(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        table_name_pattern: typing.Optional[str],
        types: list,
    ) -> str:
        sql: str = (
            "SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT,"
            " schemaname AS table_schem,"
            " tablename AS TABLE_NAME,"
            " 'EXTERNAL TABLE' AS TABLE_TYPE,"
            " NULL AS REMARKS,"
            " '' as TYPE_CAT,"
            " '' as TYPE_SCHEM,"
            " '' as TYPE_NAME, "
            " '' AS SELF_REFERENCING_COL_NAME,"
            " '' AS REF_GENERATION "
            " FROM svv_external_tables)"
            " WHERE true "
        )
        filter_clause: str = self.__get_table_filter_clause(
            catalog, schema_pattern, table_name_pattern, types, "EXTERNAL_SCHEMA_QUERY"
        )
        orderby: str = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME "
        sql += filter_clause + orderby
        return sql

    def get_columns(
        self: "Cursor",
        catalog: typing.Optional[str] = None,
        schema_pattern: typing.Optional[str] = None,
        tablename_pattern: typing.Optional[str] = None,
        columnname_pattern: typing.Optional[str] = None,
    ) -> tuple:
        """Returns a list of all columns in a specific table in Amazon Redshift database"""
        sql: str = ""
        schema_pattern_type: str = self.__schema_pattern_match(schema_pattern)
        if schema_pattern_type == "LOCAL_SCHEMA_QUERY":
            sql = self.__build_local_schema_columns_query(
                catalog, schema_pattern, tablename_pattern, columnname_pattern
            )
        elif schema_pattern_type == "NO_SCHEMA_UNIVERSAL_QUERY":
            sql = self.__build_universal_schema_columns_query(
                catalog, schema_pattern, tablename_pattern, columnname_pattern
            )
        elif schema_pattern_type == "EXTERNAL_SCHEMA_QUERY":
            sql = self.__build_external_schema_columns_query(
                catalog, schema_pattern, tablename_pattern, columnname_pattern
            )

        self.execute(sql)
        columns: tuple = self.fetchall()
        return columns

    def __build_local_schema_columns_query(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        tablename_pattern: typing.Optional[str],
        columnname_pattern: typing.Optional[str],
    ) -> str:
        sql: str = (
            "SELECT * FROM ( "
            "SELECT current_database() AS TABLE_CAT, "
            "n.nspname AS TABLE_SCHEM, "
            "c.relname as TABLE_NAME , "
            "a.attname as COLUMN_NAME, "
            "CAST(case typname "
            "when 'text' THEN 12 "
            "when 'bit' THEN -7 "
            "when 'bool' THEN -7 "
            "when 'boolean' THEN -7 "
            "when 'varchar' THEN 12 "
            "when 'character varying' THEN 12 "
            "when 'char' THEN 1 "
            "when '\"char\"' THEN 1 "
            "when 'character' THEN 1 "
            "when 'nchar' THEN 12 "
            "when 'bpchar' THEN 1 "
            "when 'nvarchar' THEN 12 "
            "when 'date' THEN 91 "
            "when 'timestamp' THEN 93 "
            "when 'timestamp without time zone' THEN 93 "
            "when 'smallint' THEN 5 "
            "when 'int2' THEN 5 "
            "when 'integer' THEN 4 "
            "when 'int' THEN 4 "
            "when 'int4' THEN 4 "
            "when 'bigint' THEN -5 "
            "when 'int8' THEN -5 "
            "when 'decimal' THEN 3 "
            "when 'real' THEN 7 "
            "when 'float4' THEN 7 "
            "when 'double precision' THEN 8 "
            "when 'float8' THEN 8 "
            "when 'float' THEN 6 "
            "when 'numeric' THEN 2 "
            "when '_float4' THEN 2003 "
            "when 'timestamptz' THEN 2014 "
            "when 'timestamp with time zone' THEN 2014 "
            "when '_aclitem' THEN 2003 "
            "when '_text' THEN 2003 "
            "when 'bytea' THEN -2 "
            "when 'oid' THEN -5 "
            "when 'name' THEN 12 "
            "when '_int4' THEN 2003 "
            "when '_int2' THEN 2003 "
            "when 'ARRAY' THEN 2003 "
            "when 'geometry' THEN -4 "
            "when 'omni' THEN -16 "
            "else 1111 END as SMALLINT) AS DATA_TYPE, "
            "t.typname as TYPE_NAME, "
            "case typname "
            "when 'int4' THEN 10 "
            "when 'bit' THEN 1 "
            "when 'bool' THEN 1 "
            "when 'varchar' THEN atttypmod -4 "
            "when 'character varying' THEN atttypmod -4 "
            "when 'char' THEN atttypmod -4 "
            "when 'character' THEN atttypmod -4 "
            "when 'nchar' THEN atttypmod -4 "
            "when 'bpchar' THEN atttypmod -4 "
            "when 'nvarchar' THEN atttypmod -4 "
            "when 'date' THEN 13 "
            "when 'timestamp' THEN 29 "
            "when 'smallint' THEN 5 "
            "when 'int2' THEN 5 "
            "when 'integer' THEN 10 "
            "when 'int' THEN 10 "
            "when 'int4' THEN 10 "
            "when 'bigint' THEN 19 "
            "when 'int8' THEN 19 "
            "when 'decimal' then (atttypmod - 4) >> 16 "
            "when 'real' THEN 8 "
            "when 'float4' THEN 8 "
            "when 'double precision' THEN 17 "
            "when 'float8' THEN 17 "
            "when 'float' THEN 17 "
            "when 'numeric' THEN (atttypmod - 4) >> 16 "
            "when '_float4' THEN 8 "
            "when 'timestamptz' THEN 35 "
            "when 'oid' THEN 10 "
            "when '_int4' THEN 10 "
            "when '_int2' THEN 5 "
            "when 'geometry' THEN NULL "
            "when 'omni' THEN NULL "
            "else 2147483647 end as COLUMN_SIZE , "
            "null as BUFFER_LENGTH , "
            "case typname "
            "when 'float4' then 8 "
            "when 'float8' then 17 "
            "when 'numeric' then (atttypmod - 4) & 65535 "
            "when 'timestamp' then 6 "
            "when 'geometry' then NULL "
            "when 'omni' then NULL "
            "else 0 end as DECIMAL_DIGITS, "
            "10 AS NUM_PREC_RADIX , "
            "case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) "
            "when 'false' then 1 "
            "when NULL then 2 "
            "else 0 end AS NULLABLE , "
            "dsc.description as REMARKS , "
            "pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS COLUMN_DEF, "
            "CAST(case typname "
            "when 'text' THEN 12 "
            "when 'bit' THEN -7 "
            "when 'bool' THEN -7 "
            "when 'boolean' THEN -7 "
            "when 'varchar' THEN 12 "
            "when 'character varying' THEN 12 "
            "when '\"char\"' THEN 1 "
            "when 'char' THEN 1 "
            "when 'character' THEN 1 "
            "when 'nchar' THEN 1 "
            "when 'bpchar' THEN 1 "
            "when 'nvarchar' THEN 12 "
            "when 'date' THEN 91 "
            "when 'timestamp' THEN 93 "
            "when 'timestamp without time zone' THEN 93 "
            "when 'smallint' THEN 5 "
            "when 'int2' THEN 5 "
            "when 'integer' THEN 4 "
            "when 'int' THEN 4 "
            "when 'int4' THEN 4 "
            "when 'bigint' THEN -5 "
            "when 'int8' THEN -5 "
            "when 'decimal' THEN 3 "
            "when 'real' THEN 7 "
            "when 'float4' THEN 7 "
            "when 'double precision' THEN 8 "
            "when 'float8' THEN 8 "
            "when 'float' THEN 6 "
            "when 'numeric' THEN 2 "
            "when '_float4' THEN 2003 "
            "when 'timestamptz' THEN 2014 "
            "when 'timestamp with time zone' THEN 2014 "
            "when '_aclitem' THEN 2003 "
            "when '_text' THEN 2003 "
            "when 'bytea' THEN -2 "
            "when 'oid' THEN -5 "
            "when 'name' THEN 12 "
            "when '_int4' THEN 2003 "
            "when '_int2' THEN 2003 "
            "when 'ARRAY' THEN 2003 "
            "when 'geometry' THEN -4 "
            "when 'omni' THEN -16 "
            "else 1111 END as SMALLINT) AS SQL_DATA_TYPE, "
            "CAST(NULL AS SMALLINT) as SQL_DATETIME_SUB , "
            "case typname "
            "when 'int4' THEN 10 "
            "when 'bit' THEN 1 "
            "when 'bool' THEN 1 "
            "when 'varchar' THEN atttypmod -4 "
            "when 'character varying' THEN atttypmod -4 "
            "when 'char' THEN atttypmod -4 "
            "when 'character' THEN atttypmod -4 "
            "when 'nchar' THEN atttypmod -4 "
            "when 'bpchar' THEN atttypmod -4 "
            "when 'nvarchar' THEN atttypmod -4 "
            "when 'date' THEN 13 "
            "when 'timestamp' THEN 29 "
            "when 'smallint' THEN 5 "
            "when 'int2' THEN 5 "
            "when 'integer' THEN 10 "
            "when 'int' THEN 10 "
            "when 'int4' THEN 10 "
            "when 'bigint' THEN 19 "
            "when 'int8' THEN 19 "
            "when 'decimal' then ((atttypmod - 4) >> 16) & 65535 "
            "when 'real' THEN 8 "
            "when 'float4' THEN 8 "
            "when 'double precision' THEN 17 "
            "when 'float8' THEN 17 "
            "when 'float' THEN 17 "
            "when 'numeric' THEN ((atttypmod - 4) >> 16) & 65535 "
            "when '_float4' THEN 8 "
            "when 'timestamptz' THEN 35 "
            "when 'oid' THEN 10 "
            "when '_int4' THEN 10 "
            "when '_int2' THEN 5 "
            "when 'geometry' THEN NULL "
            "when 'omni' THEN NULL "
            "else 2147483647 end as CHAR_OCTET_LENGTH , "
            "a.attnum AS ORDINAL_POSITION, "
            "case a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) "
            "when 'false' then 'YES' "
            "when NULL then '' "
            "else 'NO' end AS IS_NULLABLE, "
            "null as SCOPE_CATALOG , "
            "null as SCOPE_SCHEMA , "
            "null as SCOPE_TABLE, "
            "t.typbasetype AS SOURCE_DATA_TYPE , "
            "CASE WHEN left(pg_catalog.pg_get_expr(def.adbin, def.adrelid), 16) = 'default_identity' THEN 'YES' "
            "ELSE 'NO' END AS IS_AUTOINCREMENT, "
            "IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN "
            "FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) "
            "JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) "
            "JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) "
            "LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) "
            "LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) "
            "LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') "
            "LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') "
            "WHERE a.attnum > 0 AND NOT a.attisdropped    "
        )
        if schema_pattern is not None:
            sql += " AND n.nspname LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        elif tablename_pattern is not None:
            sql += " AND c.relname LIKE {table}".format(table=self.__escape_quotes(tablename_pattern))
        elif columnname_pattern is not None:
            sql += " AND attname LIKE {column}".format(column=self.__escape_quotes(columnname_pattern))

        sql += " ORDER BY TABLE_SCHEM,c.relname,attnum ) "

        # This part uses redshift method PG_GET_LATE_BINDING_VIEW_COLS() to
        # get the column list for late binding view.
        sql += (
            " UNION ALL "
            "SELECT current_database()::VARCHAR(128) AS TABLE_CAT, "
            "schemaname::varchar(128) AS table_schem, "
            "tablename::varchar(128) AS table_name, "
            "columnname::varchar(128) AS column_name, "
            "CAST(CASE columntype_rep "
            "WHEN 'text' THEN 12 "
            "WHEN 'bit' THEN -7 "
            "WHEN 'bool' THEN -7 "
            "WHEN 'boolean' THEN -7 "
            "WHEN 'varchar' THEN 12 "
            "WHEN 'character varying' THEN 12 "
            "WHEN 'char' THEN 1 "
            "WHEN 'character' THEN 1 "
            "WHEN 'nchar' THEN 1 "
            "WHEN 'bpchar' THEN 1 "
            "WHEN 'nvarchar' THEN 12 "
            "WHEN '\"char\"' THEN 1 "
            "WHEN 'date' THEN 91 "
            "WHEN 'timestamp' THEN 93 "
            "WHEN 'timestamp without time zone' THEN 93 "
            "WHEN 'timestamp with time zone' THEN 2014 "
            "WHEN 'smallint' THEN 5 "
            "WHEN 'int2' THEN 5 "
            "WHEN 'integer' THEN 4 "
            "WHEN 'int' THEN 4 "
            "WHEN 'int4' THEN 4 "
            "WHEN 'bigint' THEN -5 "
            "WHEN 'int8' THEN -5 "
            "WHEN 'decimal' THEN 3 "
            "WHEN 'real' THEN 7 "
            "WHEN 'float4' THEN 7 "
            "WHEN 'double precision' THEN 8 "
            "WHEN 'float8' THEN 8 "
            "WHEN 'float' THEN 6 "
            "WHEN 'numeric' THEN 2 "
            "WHEN 'timestamptz' THEN 2014 "
            "WHEN 'bytea' THEN -2 "
            "WHEN 'oid' THEN -5 "
            "WHEN 'name' THEN 12 "
            "WHEN 'ARRAY' THEN 2003 "
            "WHEN 'geometry' THEN -4 "
            "WHEN 'omni' THEN -16 "
            "ELSE 1111 END AS SMALLINT) AS DATA_TYPE, "
            "COALESCE(NULL,CASE columntype WHEN 'boolean' THEN 'bool' "
            "WHEN 'character varying' THEN 'varchar' "
            "WHEN '\"char\"' THEN 'char' "
            "WHEN 'smallint' THEN 'int2' "
            "WHEN 'integer' THEN 'int4'"
            "WHEN 'bigint' THEN 'int8' "
            "WHEN 'real' THEN 'float4' "
            "WHEN 'double precision' THEN 'float8' "
            "WHEN 'timestamp without time zone' THEN 'timestamp' "
            "WHEN 'timestamp with time zone' THEN 'timestamptz' "
            "ELSE columntype END) AS TYPE_NAME,  "
            "CASE columntype_rep "
            "WHEN 'int4' THEN 10  "
            "WHEN 'bit' THEN 1    "
            "WHEN 'bool' THEN 1"
            "WHEN 'boolean' THEN 1"
            "WHEN 'varchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN 'character varying' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN 'char' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER "
            "WHEN 'character' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER "
            "WHEN 'nchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN 'bpchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN 'nvarchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN 'date' THEN 13 "
            "WHEN 'timestamp' THEN 29 "
            "WHEN 'timestamp without time zone' THEN 29 "
            "WHEN 'smallint' THEN 5 "
            "WHEN 'int2' THEN 5 "
            "WHEN 'integer' THEN 10 "
            "WHEN 'int' THEN 10 "
            "WHEN 'int4' THEN 10 "
            "WHEN 'bigint' THEN 19 "
            "WHEN 'int8' THEN 19 "
            "WHEN 'decimal' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN 'real' THEN 8 "
            "WHEN 'float4' THEN 8 "
            "WHEN 'double precision' THEN 17 "
            "WHEN 'float8' THEN 17 "
            "WHEN 'float' THEN 17"
            "WHEN 'numeric' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN '_float4' THEN 8 "
            "WHEN 'timestamptz' THEN 35 "
            "WHEN 'timestamp with time zone' THEN 35 "
            "WHEN 'oid' THEN 10 "
            "WHEN '_int4' THEN 10 "
            "WHEN '_int2' THEN 5 "
            "WHEN 'geometry' THEN NULL "
            "WHEN 'omni' THEN NULL "
            "ELSE 2147483647 END AS COLUMN_SIZE, "
            "NULL AS BUFFER_LENGTH, "
            "CASE columntype "
            "WHEN 'real' THEN 8 "
            "WHEN 'float4' THEN 8 "
            "WHEN 'double precision' THEN 17 "
            "WHEN 'float8' THEN 17 "
            "WHEN 'timestamp' THEN 6 "
            "WHEN 'timestamp without time zone' THEN 6 "
            "WHEN 'geometry' THEN NULL "
            "WHEN 'omni' THEN NULL "
            "ELSE 0 END AS DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX, "
            "NULL AS NULLABLE,  NULL AS REMARKS,   NULL AS COLUMN_DEF, "
            "CAST(CASE columntype_rep "
            "WHEN 'text' THEN 12 "
            "WHEN 'bit' THEN -7 "
            "WHEN 'bool' THEN -7 "
            "WHEN 'boolean' THEN -7 "
            "WHEN 'varchar' THEN 12 "
            "WHEN 'character varying' THEN 12 "
            "WHEN 'char' THEN 1 "
            "WHEN 'character' THEN 1 "
            "WHEN 'nchar' THEN 12 "
            "WHEN 'bpchar' THEN 1 "
            "WHEN 'nvarchar' THEN 12 "
            "WHEN '\"char\"' THEN 1 "
            "WHEN 'date' THEN 91 "
            "WHEN 'timestamp' THEN 93 "
            "WHEN 'timestamp without time zone' THEN 93 "
            "WHEN 'timestamp with time zone' THEN 2014 "
            "WHEN 'smallint' THEN 5 "
            "WHEN 'int2' THEN 5 "
            "WHEN 'integer' THEN 4 "
            "WHEN 'int' THEN 4 "
            "WHEN 'int4' THEN 4 "
            "WHEN 'bigint' THEN -5 "
            "WHEN 'int8' THEN -5 "
            "WHEN 'decimal' THEN 3 "
            "WHEN 'real' THEN 7 "
            "WHEN 'float4' THEN 7 "
            "WHEN 'double precision' THEN 8 "
            "WHEN 'float8' THEN 8 "
            "WHEN 'float' THEN 6 "
            "WHEN 'numeric' THEN 2 "
            "WHEN 'timestamptz' THEN 2014 "
            "WHEN 'bytea' THEN -2 "
            "WHEN 'oid' THEN -5 "
            "WHEN 'name' THEN 12 "
            "WHEN 'ARRAY' THEN 2003 "
            "WHEN 'geometry' THEN -4 "
            "WHEN 'omni' THEN -4 "
            "ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE, "
            "CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB, CASE "
            "WHEN LEFT (columntype,7) = 'varchar' THEN regexp_substr (columntype,'[0-9]+',7)::INTEGER "
            "WHEN LEFT (columntype,4) = 'char' THEN regexp_substr (columntype,'[0-9]+',4)::INTEGER "
            "WHEN columntype = 'string' THEN 16383  ELSE NULL "
            "END AS CHAR_OCTET_LENGTH, columnnum AS ORDINAL_POSITION, "
            "NULL AS IS_NULLABLE,  NULL AS SCOPE_CATALOG,  NULL AS SCOPE_SCHEMA, "
            "NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, 'NO' AS IS_AUTOINCREMENT, "
            "'NO' as IS_GENERATEDCOLUMN "
            "FROM (select lbv_cols.schemaname, "
            "lbv_cols.tablename, lbv_cols.columnname,"
            "REGEXP_REPLACE(REGEXP_REPLACE(lbv_cols.columntype,'\\\\(.*\\\\)'),'^_.+','ARRAY') as columntype_rep,"
            "columntype, "
            "lbv_cols.columnnum "
            "from pg_get_late_binding_view_cols() lbv_cols( "
            "schemaname name, tablename name, columnname name, "
            "columntype text, columnnum int)) lbv_columns  "
            " WHERE true "
        )
        if schema_pattern is not None:
            sql += " AND schemaname LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        if tablename_pattern is not None:
            sql += " AND tablename LIKE {table}".format(table=self.__escape_quotes(tablename_pattern))
        if columnname_pattern is not None:
            sql += " AND columnname LIKE {column}".format(column=self.__escape_quotes(columnname_pattern))

        return sql

    def __build_universal_schema_columns_query(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        tablename_pattern: typing.Optional[str],
        columnname_pattern: typing.Optional[str],
    ) -> str:
        unknown_column_size: str = "2147483647"
        sql: str = (
            "SELECT current_database()::varchar(128) AS TABLE_CAT,"
            " table_schema AS TABLE_SCHEM,"
            " table_name,"
            " COLUMN_NAME,"
            " CAST(CASE regexp_replace(data_type, '^_.+', 'ARRAY')"
            " WHEN 'text' THEN 12"
            " WHEN 'bit' THEN -7"
            " WHEN 'bool' THEN -7"
            " WHEN 'boolean' THEN -7"
            " WHEN 'varchar' THEN 12"
            " WHEN 'character varying' THEN 12"
            " WHEN 'char' THEN 1"
            " WHEN 'character' THEN 1"
            " WHEN 'nchar' THEN 1"
            " WHEN 'bpchar' THEN 1"
            " WHEN 'nvarchar' THEN 12"
            " WHEN '\"char\"' THEN 1"
            " WHEN 'date' THEN 91"
            " WHEN 'timestamp' THEN 93"
            " WHEN 'timestamp without time zone' THEN 93"
            " WHEN 'timestamp with time zone' THEN 2014"
            " WHEN 'smallint' THEN 5"
            " WHEN 'int2' THEN 5"
            " WHEN 'integer' THEN 4"
            " WHEN 'int' THEN 4"
            " WHEN 'int4' THEN 4"
            " WHEN 'bigint' THEN -5"
            " WHEN 'int8' THEN -5"
            " WHEN 'decimal' THEN 3"
            " WHEN 'real' THEN 7"
            " WHEN 'float4' THEN 7"
            " WHEN 'double precision' THEN 8"
            " WHEN 'float8' THEN 8"
            " WHEN 'float' THEN 6"
            " WHEN 'numeric' THEN 2"
            " WHEN 'timestamptz' THEN 2014"
            " WHEN 'bytea' THEN -2"
            " WHEN 'oid' THEN -5"
            " WHEN 'name' THEN 12"
            " WHEN 'ARRAY' THEN 2003"
            " WHEN 'geometry' THEN -4 "
            " WHEN 'omni' THEN -16 "
            " ELSE 1111 END AS SMALLINT) AS DATA_TYPE,"
            " COALESCE("
            " domain_name,"
            " CASE data_type"
            " WHEN 'boolean' THEN 'bool'"
            " WHEN 'character varying' THEN 'varchar'"
            " WHEN '\"char\"' THEN 'char'"
            " WHEN 'smallint' THEN 'int2'"
            " WHEN 'integer' THEN 'int4'"
            " WHEN 'bigint' THEN 'int8'"
            " WHEN 'real' THEN 'float4'"
            " WHEN 'double precision' THEN 'float8'"
            " WHEN 'timestamp without time zone' THEN 'timestamp'"
            " WHEN 'timestamp with time zone' THEN 'timestamptz'"
            " ELSE data_type"
            " END) AS TYPE_NAME,"
            " CASE data_type"
            " WHEN 'int4' THEN 10"
            " WHEN 'bit' THEN 1"
            " WHEN 'bool' THEN 1"
            " WHEN 'boolean' THEN 1"
            " WHEN 'varchar' THEN character_maximum_length"
            " WHEN 'character varying' THEN character_maximum_length"
            " WHEN 'char' THEN character_maximum_length"
            " WHEN 'character' THEN character_maximum_length"
            " WHEN 'nchar' THEN character_maximum_length"
            " WHEN 'bpchar' THEN character_maximum_length"
            " WHEN 'nvarchar' THEN character_maximum_length"
            " WHEN 'date' THEN 13"
            " WHEN 'timestamp' THEN 29"
            " WHEN 'timestamp without time zone' THEN 29"
            " WHEN 'smallint' THEN 5"
            " WHEN 'int2' THEN 5"
            " WHEN 'integer' THEN 10"
            " WHEN 'int' THEN 10"
            " WHEN 'int4' THEN 10"
            " WHEN 'bigint' THEN 19"
            " WHEN 'int8' THEN 19"
            " WHEN 'decimal' THEN numeric_precision"
            " WHEN 'real' THEN 8"
            " WHEN 'float4' THEN 8"
            " WHEN 'double precision' THEN 17"
            " WHEN 'float8' THEN 17"
            " WHEN 'float' THEN 17"
            " WHEN 'numeric' THEN numeric_precision"
            " WHEN '_float4' THEN 8"
            " WHEN 'timestamptz' THEN 35"
            " WHEN 'timestamp with time zone' THEN 35"
            " WHEN 'oid' THEN 10"
            " WHEN '_int4' THEN 10"
            " WHEN '_int2' THEN 5"
            " WHEN 'geometry' THEN NULL"
            " WHEN 'omni' THEN NULL"
            " ELSE {unknown_column_size}"
            " END AS COLUMN_SIZE,"
            " NULL AS BUFFER_LENGTH,"
            " CASE data_type"
            " WHEN 'real' THEN 8"
            " WHEN 'float4' THEN 8"
            " WHEN 'double precision' THEN 17"
            " WHEN 'float8' THEN 17"
            " WHEN 'numeric' THEN numeric_scale"
            " WHEN 'timestamp' THEN 6"
            " WHEN 'timestamp without time zone' THEN 6"
            " WHEN 'geometry' THEN NULL"
            " WHEN 'omni' THEN NULL"
            " ELSE 0"
            " END AS DECIMAL_DIGITS,"
            " 10 AS NUM_PREC_RADIX,"
            " CASE is_nullable WHEN 'YES' THEN 1"
            " WHEN 'NO' THEN 0"
            " ELSE 2 end AS NULLABLE,"
            " REMARKS,"
            " column_default AS COLUMN_DEF,"
            " CAST(CASE regexp_replace(data_type, '^_.+', 'ARRAY')"
            " WHEN 'text' THEN 12"
            " WHEN 'bit' THEN -7"
            " WHEN 'bool' THEN -7"
            " WHEN 'boolean' THEN -7"
            " WHEN 'varchar' THEN 12"
            " WHEN 'character varying' THEN 12"
            " WHEN 'char' THEN 1"
            " WHEN 'character' THEN 1"
            " WHEN 'nchar' THEN 1"
            " WHEN 'bpchar' THEN 1"
            " WHEN 'nvarchar' THEN 12"
            " WHEN '\"char\"' THEN 1"
            " WHEN 'date' THEN 91"
            " WHEN 'timestamp' THEN 93"
            " WHEN 'timestamp without time zone' THEN 93"
            " WHEN 'timestamp with time zone' THEN 2014"
            " WHEN 'smallint' THEN 5"
            " WHEN 'int2' THEN 5"
            " WHEN 'integer' THEN 4"
            " WHEN 'int' THEN 4"
            " WHEN 'int4' THEN 4"
            " WHEN 'bigint' THEN -5"
            " WHEN 'int8' THEN -5"
            " WHEN 'decimal' THEN 3"
            " WHEN 'real' THEN 7"
            " WHEN 'float4' THEN 7"
            " WHEN 'double precision' THEN 8"
            " WHEN 'float8' THEN 8"
            " WHEN 'float' THEN 6"
            " WHEN 'numeric' THEN 2"
            " WHEN 'timestamptz' THEN 2014"
            " WHEN 'bytea' THEN -2"
            " WHEN 'oid' THEN -5"
            " WHEN 'name' THEN 12"
            " WHEN 'ARRAY' THEN 2003"
            " WHEN 'geometry' THEN -4"
            " WHEN 'omni' THEN -16"
            " ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE,"
            " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,"
            " CASE data_type"
            " WHEN 'int4' THEN 10"
            " WHEN 'bit' THEN 1"
            " WHEN 'bool' THEN 1"
            " WHEN 'boolean' THEN 1"
            " WHEN 'varchar' THEN character_maximum_length"
            " WHEN 'character varying' THEN character_maximum_length"
            " WHEN 'char' THEN character_maximum_length"
            " WHEN 'character' THEN character_maximum_length"
            " WHEN 'nchar' THEN character_maximum_length"
            " WHEN 'bpchar' THEN character_maximum_length"
            " WHEN 'nvarchar' THEN character_maximum_length"
            " WHEN 'date' THEN 13"
            " WHEN 'timestamp' THEN 29"
            " WHEN 'timestamp without time zone' THEN 29"
            " WHEN 'smallint' THEN 5"
            " WHEN 'int2' THEN 5"
            " WHEN 'integer' THEN 10"
            " WHEN 'int' THEN 10"
            " WHEN 'int4' THEN 10"
            " WHEN 'bigint' THEN 19"
            " WHEN 'int8' THEN 19"
            " WHEN 'decimal' THEN numeric_precision"
            " WHEN 'real' THEN 8"
            " WHEN 'float4' THEN 8"
            " WHEN 'double precision' THEN 17"
            " WHEN 'float8' THEN 17"
            " WHEN 'float' THEN 17"
            " WHEN 'numeric' THEN numeric_precision"
            " WHEN '_float4' THEN 8"
            " WHEN 'timestamptz' THEN 35"
            " WHEN 'timestamp with time zone' THEN 35"
            " WHEN 'oid' THEN 10"
            " WHEN '_int4' THEN 10"
            " WHEN '_int2' THEN 5"
            " WHEN 'geometry' THEN NULL"
            " WHEN 'omni' THEN NULL"
            " ELSE {unknown_column_size}"
            " END AS CHAR_OCTET_LENGTH,"
            " ordinal_position AS ORDINAL_POSITION,"
            " is_nullable AS IS_NULLABLE,"
            " NULL AS SCOPE_CATALOG,"
            " NULL AS SCOPE_SCHEMA,"
            " NULL AS SCOPE_TABLE,"
            " CASE"
            " WHEN domain_name is not null THEN data_type"
            " END AS SOURCE_DATA_TYPE,"
            " CASE WHEN left(column_default, 10) = '\\\"identity\\\"' THEN 'YES'"
            " WHEN left(column_default, 16) = 'default_identity' THEN 'YES' "
            " ELSE 'NO' END AS IS_AUTOINCREMENT,"
            " IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN"
            " FROM svv_columns"
            " WHERE true "
        ).format(unknown_column_size=unknown_column_size)

        if schema_pattern is not None:
            sql += " AND table_schema LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        if tablename_pattern is not None:
            sql += " AND table_name LIKE {table}".format(table=self.__escape_quotes(tablename_pattern))
        if columnname_pattern is not None:
            sql += " AND COLUMN_NAME LIKE {column}".format(column=self.__escape_quotes(columnname_pattern))

        sql += " ORDER BY table_schem,table_name,ORDINAL_POSITION "
        return sql

    def __build_external_schema_columns_query(
        self: "Cursor",
        catalog: typing.Optional[str],
        schema_pattern: typing.Optional[str],
        tablename_pattern: typing.Optional[str],
        columnname_pattern: typing.Optional[str],
    ) -> str:
        sql: str = (
            "SELECT current_database()::varchar(128) AS TABLE_CAT,"
            " schemaname AS TABLE_SCHEM,"
            " tablename AS TABLE_NAME,"
            " columnname AS COLUMN_NAME,"
            " CAST(CASE WHEN external_type = 'text' THEN 12"
            " WHEN external_type = 'bit' THEN -7"
            " WHEN external_type = 'bool' THEN -7"
            " WHEN external_type = 'boolean' THEN -7"
            " WHEN left(external_type, 7) = 'varchar' THEN 12"
            " WHEN left(external_type, 17) = 'character varying' THEN 12"
            " WHEN left(external_type, 4) = 'char' THEN 1"
            " WHEN left(external_type, 9) = 'character' THEN 1"
            " WHEN left(external_type, 5) = 'nchar' THEN 1"
            " WHEN left(external_type, 6) = 'bpchar' THEN 1"
            " WHEN left(external_type, 8) = 'nvarchar' THEN 12"
            " WHEN external_type = '\"char\"' THEN 1"
            " WHEN external_type = 'date' THEN 91"
            " WHEN external_type = 'timestamp' THEN 93"
            " WHEN external_type = 'timestamp without time zone' THEN 93"
            " WHEN external_type = 'timestamp with time zone' THEN 2014"
            " WHEN external_type = 'smallint' THEN 5"
            " WHEN external_type = 'int2' THEN 5"
            " WHEN external_type = '_int2' THEN 5"
            " WHEN external_type = 'integer' THEN 4"
            " WHEN external_type = 'int' THEN 4"
            " WHEN external_type = 'int4' THEN 4"
            " WHEN external_type = '_int4' THEN 4"
            " WHEN external_type = 'bigint' THEN -5"
            " WHEN external_type = 'int8' THEN -5"
            " WHEN left(external_type, 7) = 'decimal' THEN 2"
            " WHEN external_type = 'real' THEN 7"
            " WHEN external_type = 'float4' THEN 7"
            " WHEN external_type = '_float4' THEN 7"
            " WHEN external_type = 'double' THEN 8"
            " WHEN external_type = 'double precision' THEN 8"
            " WHEN external_type = 'float8' THEN 8"
            " WHEN external_type = '_float8' THEN 8"
            " WHEN external_type = 'float' THEN 6"
            " WHEN left(external_type, 7) = 'numeric' THEN 2"
            " WHEN external_type = 'timestamptz' THEN 2014"
            " WHEN external_type = 'bytea' THEN -2"
            " WHEN external_type = 'oid' THEN -5"
            " WHEN external_type = 'name' THEN 12"
            " WHEN external_type = 'ARRAY' THEN 2003"
            " WHEN external_type = 'geometry' THEN -4"
            " WHEN external_type = 'omni' THEN -16"
            " ELSE 1111 END AS SMALLINT) AS DATA_TYPE,"
            " CASE WHEN left(external_type, 17) = 'character varying' THEN 'varchar'"
            " WHEN left(external_type, 7) = 'varchar' THEN 'varchar'"
            " WHEN left(external_type, 4) = 'char' THEN 'char'"
            " WHEN left(external_type, 7) = 'decimal' THEN 'numeric'"
            " WHEN left(external_type, 7) = 'numeric' THEN 'numeric'"
            " WHEN external_type = 'double' THEN 'double precision'"
            " WHEN external_type = 'timestamp without time zone' THEN 'timestamp'"
            " WHEN external_type = 'timestamp with time zone' THEN 'timestamptz'"
            " ELSE external_type END AS TYPE_NAME,"
            " CASE WHEN external_type = 'int4' THEN 10"
            " WHEN external_type = 'bit' THEN 1"
            " WHEN external_type = 'bool' THEN 1"
            " WHEN external_type = 'boolean' THEN 1"
            " WHEN left(external_type, 7) = 'varchar' THEN regexp_substr(external_type, '[0-9]+', 7)::integer"
            " WHEN left(external_type, 17) = 'character varying' THEN regexp_substr(external_type, '[0-9]+', 17)::integer"
            " WHEN left(external_type, 4) = 'char' THEN regexp_substr(external_type, '[0-9]+', 4)::integer"
            " WHEN left(external_type, 9) = 'character' THEN regexp_substr(external_type, '[0-9]+', 9)::integer"
            " WHEN left(external_type, 5) = 'nchar' THEN regexp_substr(external_type, '[0-9]+', 5)::integer"
            " WHEN left(external_type, 6) = 'bpchar' THEN regexp_substr(external_type, '[0-9]+', 6)::integer"
            " WHEN left(external_type, 8) = 'nvarchar' THEN regexp_substr(external_type, '[0-9]+', 8)::integer"
            " WHEN external_type = 'date' THEN 13 WHEN external_type = 'timestamp' THEN 29"
            " WHEN external_type = 'timestamp without time zone' THEN 29"
            " WHEN external_type = 'smallint' THEN 5"
            " WHEN external_type = 'int2' THEN 5"
            " WHEN external_type = 'integer' THEN 10"
            " WHEN external_type = 'int' THEN 10"
            " WHEN external_type = 'int4' THEN 10"
            " WHEN external_type = 'bigint' THEN 19"
            " WHEN external_type = 'int8' THEN 19"
            " WHEN left(external_type, 7) = 'decimal' THEN regexp_substr(external_type, '[0-9]+', 7)::integer"
            " WHEN external_type = 'real' THEN 8"
            " WHEN external_type = 'float4' THEN 8"
            " WHEN external_type = '_float4' THEN 8"
            " WHEN external_type = 'double' THEN 17"
            " WHEN external_type = 'double precision' THEN 17"
            " WHEN external_type = 'float8' THEN 17"
            " WHEN external_type = '_float8' THEN 17"
            " WHEN external_type = 'float' THEN 17"
            " WHEN left(external_type, 7) = 'numeric' THEN regexp_substr(external_type, '[0-9]+', 7)::integer"
            " WHEN external_type = '_float4' THEN 8"
            " WHEN external_type = 'timestamptz' THEN 35"
            " WHEN external_type = 'timestamp with time zone' THEN 35"
            " WHEN external_type = 'oid' THEN 10"
            " WHEN external_type = '_int4' THEN 10"
            " WHEN external_type = '_int2' THEN 5"
            " WHEN external_type = 'geometry' THEN NULL"
            " WHEN external_type = 'omni' THEN NULL"
            " ELSE 2147483647 END AS COLUMN_SIZE,"
            " NULL AS BUFFER_LENGTH,"
            " CASE WHEN external_type = 'real'THEN 8"
            " WHEN external_type = 'float4' THEN 8"
            " WHEN external_type = 'double' THEN 17"
            " WHEN external_type = 'double precision' THEN 17"
            " WHEN external_type = 'float8' THEN 17"
            " WHEN left(external_type, 7) = 'numeric' THEN regexp_substr(external_type, '[0-9]+', 10)::integer"
            " WHEN left(external_type, 7) = 'decimal' THEN regexp_substr(external_type, '[0-9]+', 10)::integer"
            " WHEN external_type = 'timestamp' THEN 6"
            " WHEN external_type = 'timestamp without time zone' THEN 6"
            " WHEN external_type = 'geometry' THEN NULL"
            " WHEN external_type = 'omni' THEN NULL"
            " ELSE 0 END AS DECIMAL_DIGITS,"
            " 10 AS NUM_PREC_RADIX,"
            " NULL AS NULLABLE,"
            " NULL AS REMARKS,"
            " NULL AS COLUMN_DEF,"
            " CAST(CASE WHEN external_type = 'text' THEN 12"
            " WHEN external_type = 'bit' THEN -7"
            " WHEN external_type = 'bool' THEN -7"
            " WHEN external_type = 'boolean' THEN -7"
            " WHEN left(external_type, 7) = 'varchar' THEN 12"
            " WHEN left(external_type, 17) = 'character varying' THEN 12"
            " WHEN left(external_type, 4) = 'char' THEN 1"
            " WHEN left(external_type, 9) = 'character' THEN 1"
            " WHEN left(external_type, 5) = 'nchar' THEN 1"
            " WHEN left(external_type, 6) = 'bpchar' THEN 1"
            " WHEN left(external_type, 8) = 'nvarchar' THEN 12"
            " WHEN external_type = '\"char\"' THEN 1"
            " WHEN external_type = 'date' THEN 91"
            " WHEN external_type = 'timestamp' THEN 93"
            " WHEN external_type = 'timestamp without time zone' THEN 93"
            " WHEN external_type = 'timestamp with time zone' THEN 2014"
            " WHEN external_type = 'smallint' THEN 5"
            " WHEN external_type = 'int2' THEN 5"
            " WHEN external_type = '_int2' THEN 5"
            " WHEN external_type = 'integer' THEN 4"
            " WHEN external_type = 'int' THEN 4"
            " WHEN external_type = 'int4' THEN 4"
            " WHEN external_type = '_int4' THEN 4"
            " WHEN external_type = 'bigint' THEN -5"
            " WHEN external_type = 'int8' THEN -5"
            " WHEN left(external_type, 7) = 'decimal' THEN 3"
            " WHEN external_type = 'real' THEN 7"
            " WHEN external_type = 'float4' THEN 7"
            " WHEN external_type = '_float4' THEN 7"
            " WHEN external_type = 'double' THEN 8"
            " WHEN external_type = 'double precision' THEN 8"
            " WHEN external_type = 'float8' THEN 8"
            " WHEN external_type = '_float8' THEN 8"
            " WHEN external_type = 'float' THEN 6"
            " WHEN left(external_type, 7) = 'numeric' THEN 2"
            " WHEN external_type = 'timestamptz' THEN 2014"
            " WHEN external_type = 'bytea' THEN -2"
            " WHEN external_type = 'oid' THEN -5"
            " WHEN external_type = 'name' THEN 12"
            " WHEN external_type = 'ARRAY' THEN 2003"
            " WHEN external_type = 'geometry' THEN -4"
            " WHEN external_type = 'omni' THEN -16"
            " ELSE 1111 END AS SMALLINT) AS SQL_DATA_TYPE,"
            " CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,"
            " CASE WHEN left(external_type, 7) = 'varchar' THEN regexp_substr(external_type, '[0-9]+', 7)::integer"
            " WHEN left(external_type, 17) = 'character varying' THEN regexp_substr(external_type, '[0-9]+', 17)::integer"
            " WHEN left(external_type, 4) = 'char' THEN regexp_substr(external_type, '[0-9]+', 4)::integer"
            " WHEN left(external_type, 9) = 'character' THEN regexp_substr(external_type, '[0-9]+', 9)::integer"
            " WHEN left(external_type, 5) = 'nchar' THEN regexp_substr(external_type, '[0-9]+', 5)::integer"
            " WHEN left(external_type, 6) = 'bpchar' THEN regexp_substr(external_type, '[0-9]+', 6)::integer"
            " WHEN left(external_type, 8) = 'nvarchar' THEN regexp_substr(external_type, '[0-9]+', 8)::integer"
            " WHEN external_type = 'string' THEN 16383"
            " ELSE NULL END AS CHAR_OCTET_LENGTH,"
            " columnnum AS ORDINAL_POSITION,"
            " NULL AS IS_NULLABLE,"
            " NULL AS SCOPE_CATALOG,"
            " NULL AS SCOPE_SCHEMA,"
            " NULL AS SCOPE_TABLE,"
            " NULL AS SOURCE_DATA_TYPE,"
            " 'NO' AS IS_AUTOINCREMENT,"
            " 'NO' AS IS_GENERATEDCOLUMN"
            " FROM svv_external_columns"
            " WHERE true "
        )
        if schema_pattern is not None:
            sql += " AND schemaname LIKE {schema}".format(schema=self.__escape_quotes(schema_pattern))
        if tablename_pattern is not None:
            sql += " AND tablename LIKE {table}".format(table=self.__escape_quotes(tablename_pattern))
        if columnname_pattern is not None:
            sql += " AND columnname LIKE {column}".format(column=self.__escape_quotes(columnname_pattern))

        sql += " ORDER BY table_schem,table_name,ORDINAL_POSITION "

        return sql

    def __schema_pattern_match(self: "Cursor", schema_pattern: typing.Optional[str]) -> str:
        if schema_pattern is not None:
            sql: str = "select 1 from svv_external_schemas where schemaname like {schema}".format(
                schema=self.__escape_quotes(schema_pattern)
            )
            self.execute(sql)
            schemas: tuple = self.fetchall()
            if len(schemas) > 0:
                return "EXTERNAL_SCHEMA_QUERY"
            else:
                return "LOCAL_SCHEMA_QUERY"
        else:
            return "NO_SCHEMA_UNIVERSAL_QUERY"

    def __escape_quotes(self: "Cursor", s: str) -> str:
        return "'{s}'".format(s=s)
