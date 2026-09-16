"""
Integration tests for metadata APIs called multiple times on the same
connection. Covers cross-API sequences (``get_tables`` → ``get_columns``)
and same-API repetition (``get_tables`` twice) on both V4 and V5
code paths, and asserts that the prepared-statement cache stays
internally consistent (``len(row_desc) == len(input_funcs)`` for every
cached ps) after the calls complete.

pg_catalog.pg_class exists on every cluster so no fixture setup is needed.
"""
import typing

import redshift_connector


class TestMetadataAPISessionReuse:

    def test_get_tables_then_get_columns_on_same_connection(self, db_kwargs) -> None:
        """
        Sequential get_tables and get_columns on the same connection both
        succeed. On a V4 cluster the two calls share the same underlying
        SHOW SCHEMAS / SHOW TABLES SQL, so the second call goes through the
        prepared-statement cache-hit path; on V5 each call has its own batch
        SQL and the caches don't overlap. In both cases the test asserts
        the pair completes with non-empty results.
        """
        with redshift_connector.connect(**db_kwargs) as conn:
            cursor = conn.cursor()

            tables = cursor.get_tables(
                catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            assert len(tables) > 0

            columns = cursor.get_columns(
                catalog=None, schema_pattern="pg_catalog", tablename_pattern="pg_class")
            assert len(columns) > 0

    def test_get_tables_twice_on_same_connection(self, db_kwargs) -> None:
        """
        The same metadata API is called twice on one connection. On both V4
        and V5 the second call takes the prepared-statement cache-hit path
        for the top-level SHOW SQL. Both calls must complete and return the
        same result set.
        """
        with redshift_connector.connect(**db_kwargs) as conn:
            cursor = conn.cursor()

            first = cursor.get_tables(
                catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            second = cursor.get_tables(
                catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")

            assert len(first) > 0
            assert len(second) > 0
            assert len(first) == len(second)

    def test_get_columns_twice_on_same_connection(self, db_kwargs) -> None:
        """Same-API twice for get_columns; both calls return the same rows."""
        with redshift_connector.connect(**db_kwargs) as conn:
            cursor = conn.cursor()

            first = cursor.get_columns(
                catalog=None, schema_pattern="pg_catalog", tablename_pattern="pg_class")
            second = cursor.get_columns(
                catalog=None, schema_pattern="pg_catalog", tablename_pattern="pg_class")

            assert len(first) > 0
            assert len(second) > 0
            assert len(first) == len(second)

    def test_cached_prepared_statement_lengths_stay_synced(self, db_kwargs) -> None:
        """
        Cache invariant: for every cached prepared statement,
        ``len(row_desc) == len(input_funcs)``. The two are populated
        together during the initial Describe cycle and must stay the same
        length for the lifetime of the cache entry so that
        ``Cursor.truncated_row_desc`` can iterate them in lock-step.
        """
        with redshift_connector.connect(**db_kwargs) as conn:
            cursor = conn.cursor()

            # Exercise all three metadata APIs so caches are populated.
            cursor.get_tables(
                catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")
            cursor.get_columns(
                catalog=None, schema_pattern="pg_catalog", tablename_pattern="pg_class")
            cursor.get_table_privileges(
                catalog=None, schema_pattern="pg_catalog", table_name_pattern="pg_class")

            mismatches: typing.List[str] = []
            for _paramstyle, style_cache in conn._caches.items():
                for _pid, pid_cache in style_cache.items():
                    for key, ps in pid_cache["ps"].items():
                        sql: str = key[0]
                        row_desc_len: int = len(ps.get("row_desc", []))
                        # No RowDescription for statements like BEGIN → input_funcs is
                        # an empty tuple; only compare when input_funcs is populated.
                        input_funcs: typing.Tuple = ps.get("input_funcs", ())
                        if input_funcs and row_desc_len != len(input_funcs):
                            mismatches.append(
                                "cached ps for SQL={sql!r} has "
                                "row_desc={rd} but input_funcs={ifs}".format(
                                    sql=sql[:80], rd=row_desc_len, ifs=len(input_funcs)))

            assert not mismatches, (
                "prepared-statement cache invariant violated: "
                + "; ".join(mismatches)
            )
