import pytest

import redshift_connector


def test_lru_prepared_statements_cache(db_kwargs):
    """
    Tests LRU (Least Recently Used) behavior of prepared statements cache:
    1. Verifies cache size limits
    2. Verifies statement ordering
    3. Verifies LRU eviction policy
    4. Verifies statement reuse behavior
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        # Track statement execution order
        executed_statements = []

        # Execute 7 unique statements (exceeds cache size of 5)
        for i in range(7):
            query = f"SELECT %s::int as col1, %s::int as col2, {i} as unique_id"
            cursor.execute(query, (i, i + 1))
            executed_statements.append(query)

        # Get cache and statement queue
        cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
        statement_dict = cache["statement_dict"]

        # Basic cache size verification
        assert len(cache["ps"]) == 5, f"Cache size should be 5, but was {len(cache['ps'])}"
        assert len(statement_dict) == 5, f"Statement dict size should be 5, but was {len(statement_dict)}"

        # Verify the most recent statements are in the queue
        cached_statements = [key[0] for key in statement_dict.keys()]
        last_five_statements = executed_statements[-5:]
        assert all(stmt in cached_statements for stmt in last_five_statements), "Last 5 statements should be in cache"

        # Verify the first two statements were evicted
        first_two_statements = executed_statements[:2]
        for stmt in first_two_statements:
            assert stmt not in cached_statements, f"Statement should have been evicted: {stmt}"

        # Test statement reuse
        reuse_stmt = executed_statements[-3]
        cursor.execute(reuse_stmt, (100, 101))

        # Verify the reused statement is now at the end of the queue
        assert list(statement_dict.keys())[-1][0] == reuse_stmt, "Reused statement should be most recent"

        # Add new statement and verify LRU behavior
        new_stmt = "SELECT %s::int as col1, %s::int as col2, 999 as unique_id"
        # Track which statement should be evicted (least recently used)
        statements_before_new = [key[0] for key in statement_dict]
        cursor.execute(new_stmt, (999, 1000))

        # Verify cache size and new statement presence
        assert len(cache["ps"]) == 5, f"Cache size should still be 5, but was {len(cache['ps'])}"
        assert list(statement_dict.keys())[-1][0] == new_stmt, "New statement should be most recent"

        # Verify LRU eviction - the least recently used statement should be gone
        current_statements = [key[0] for key in statement_dict]
        lru_statement = statements_before_new[0]  # First statement in queue before adding new one
        assert (
            lru_statement not in current_statements
        ), f"Least recently used statement should have been evicted: {lru_statement}"

    finally:
        cursor.close()
        conn.close()


def test_cache_desync_after_ddl(db_kwargs):
    """
    Regression test for cache desync after DDL statements.

    Background:
        The driver maintains two parallel cache structures for prepared
        statement LRU management:
        - cache["ps"]: dict mapping (operation, params) → prepared statement metadata
        - cache["statement_dict"]: OrderedDict tracking LRU order of the same keys

        When a DDL (CREATE/DROP/ALTER) executes, handle_COMMAND_COMPLETE
        must clear BOTH structures. If only ps is cleared while
        statement_dict retains stale keys, the eviction logic will pop
        a stale key from statement_dict and crash with KeyError when
        looking it up in ps.

    Bug scenario (before fix):
        1. DDL executes → ps cleared to 0, statement_dict still has N stale keys
        2. N new unique queries refill ps to max_prepared_statements
        3. Next unique query triggers eviction → pops stale key from
           statement_dict → KeyError crash

    This test verifies the fix: both ps and statement_dict are cleared
    on DDL, so eviction works correctly after the cache refills.
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        # Step 1: Execute DDL. This triggers handle_COMMAND_COMPLETE
        # which clears ps. With the fix, statement_dict is also cleared.
        # Without the fix, statement_dict retains stale keys from the
        # implicit BEGIN transaction statement.
        cursor.execute("CREATE TABLE IF NOT EXISTS test_cache_desync (id INT)")

        # Step 2: Fill cache to the limit with unique queries.
        # Each query creates a new prepared statement in both ps and
        # statement_dict. Without the fix, statement_dict would now
        # have stale keys + new keys = more entries than ps.
        for i in range(5):
            cursor.execute(f"SELECT {i} AS cache_desync_col")
            cursor.fetchall()

        # Step 3: Execute one more unique query to trigger eviction.
        # Eviction pops the oldest key from statement_dict. Without
        # the fix, this would be a stale pre-DDL key that doesn't
        # exist in ps → KeyError crash.
        cursor.execute("SELECT 'eviction_trigger' AS cache_desync_col")
        result = cursor.fetchall()
        assert result is not None

        # Step 4: Verify the two cache structures remain synchronized.
        # This is the key invariant: ps and statement_dict must always
        # have the same number of entries.
        cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
        assert len(cache["ps"]) == len(cache["statement_dict"]), (
            f"Cache desync: ps has {len(cache['ps'])} entries, "
            f"statement_dict has {len(cache['statement_dict'])} entries"
        )

    finally:
        cursor.execute("DROP TABLE IF EXISTS test_cache_desync")
        cursor.close()
        conn.close()


def test_cache_desync_after_rollback(db_kwargs):
    """
    Regression test for cache desync after ROLLBACK.

    Background:
        ROLLBACK is handled by the same code path as DDL in
        handle_COMMAND_COMPLETE. When the server responds with a
        COMMAND_COMPLETE message containing "ROLLBACK", the driver
        must clear both ps and statement_dict.

    Bug scenario (before fix):
        1. Execute 3 queries → ps and statement_dict each have 3 entries
        2. ROLLBACK executes → ps cleared to 0, statement_dict still has
           3 stale keys
        3. 5 new unique queries refill ps to max_prepared_statements
        4. Next query triggers eviction → pops stale key → KeyError

    This test verifies ROLLBACK correctly clears both cache structures.
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        # Seed the cache with some statements so statement_dict has
        # entries that become stale after ROLLBACK.
        for i in range(3):
            cursor.execute(f"SELECT {i} AS rollback_test_col")
            cursor.fetchall()

        # ROLLBACK triggers handle_COMMAND_COMPLETE with command=b"ROLLBACK".
        # With the fix, both ps and statement_dict are cleared.
        # Without the fix, statement_dict retains 3 stale keys.
        conn.rollback()

        # Fill cache to the limit with new unique queries.
        for i in range(5):
            cursor.execute(f"SELECT {i + 100} AS rollback_test_col")
            cursor.fetchall()

        # Trigger eviction — would crash with KeyError if stale keys
        # from before the ROLLBACK remain in statement_dict.
        cursor.execute("SELECT 'rollback_eviction' AS rollback_test_col")
        result = cursor.fetchall()
        assert result is not None

        # Verify ps and statement_dict are synchronized.
        cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
        assert len(cache["ps"]) == len(cache["statement_dict"]), (
            f"Cache desync after ROLLBACK: ps has {len(cache['ps'])} entries, "
            f"statement_dict has {len(cache['statement_dict'])} entries"
        )

    finally:
        cursor.close()
        conn.close()


def test_cache_multiple_ddl_cycles(db_kwargs):
    """
    Regression test for multiple DDL cycles on the same connection.

    Background:
        In a dbt workflow, a single connection may execute many DDL
        statements interspersed with SELECT queries. Each DDL clears
        the cache, and the subsequent queries refill it. If stale keys
        accumulate across DDL cycles, the desync compounds.

    Bug scenario (before fix):
        Cycle 0: DDL → ps cleared, statement_dict has N0 stale keys
                 → 6 queries refill ps, statement_dict has N0 + 6 entries
        Cycle 1: DDL → ps cleared again, statement_dict has N0 + 6 stale keys
                 → 6 queries refill ps, statement_dict has N0 + 6 + 6 entries
        Cycle 2: DDL → statement_dict keeps growing with stale keys
                 → eviction eventually pops a stale key → KeyError

    This test verifies that repeated DDL cycles don't accumulate stale
    keys because each DDL clears both ps and statement_dict.
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        for cycle in range(3):
            # DDL clears both ps and statement_dict (with the fix).
            # Without the fix, stale keys accumulate across cycles.
            cursor.execute("CREATE TABLE IF NOT EXISTS test_multi_ddl (id INT)")

            # Fill cache beyond max_prepared_statements to trigger eviction.
            # 6 unique queries with max_prepared_statements=5 means 1 eviction.
            for i in range(6):
                cursor.execute(f"SELECT {cycle * 100 + i} AS multi_ddl_col")
                cursor.fetchall()

            # Verify caches stay synchronized after each cycle.
            # Without the fix, statement_dict grows by ~6 stale entries
            # per cycle, eventually causing eviction to crash.
            cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
            assert len(cache["ps"]) == len(cache["statement_dict"]), (
                f"Cache desync in cycle {cycle}: ps={len(cache['ps'])}, "
                f"statement_dict={len(cache['statement_dict'])}"
            )

    finally:
        cursor.execute("DROP TABLE IF EXISTS test_multi_ddl")
        cursor.close()
        conn.close()


def test_connection_usable_after_ddl_and_full_cache(db_kwargs):
    """
    Verifies the connection remains fully usable after DDL + cache
    fill + eviction.

    Background:
        The KeyError crash in the eviction path occurs AFTER a PARSE
        message has already been sent to the server (creating a named
        prepared statement on the server side). When the exception
        prevents the driver from completing the query lifecycle, the
        connection enters a broken state:
        - The server has a dangling prepared statement
        - The next query attempt may reuse the same statement name
        - This causes ProgrammingError: "prepared statement ... already exists"
        - Even rollback() fails, making the connection permanently unusable

    This test verifies that after DDL + cache fill + eviction, the
    connection can still:
        1. Execute rollback() successfully
        2. Execute new queries and return correct results
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS test_conn_usable (id INT)")

        # Fill cache beyond max_prepared_statements to trigger eviction.
        for i in range(6):
            cursor.execute(f"SELECT {i} AS conn_usable_col")
            cursor.fetchall()

        # Verify rollback works — before the fix, this would fail with
        # ProgrammingError if the previous eviction crashed mid-PARSE.
        conn.rollback()

        # Verify new queries work and return correct results.
        cursor.execute("SELECT 'still_alive' AS conn_usable_col")
        result = cursor.fetchall()
        assert result[0][0] == "still_alive"

    finally:
        cursor.execute("DROP TABLE IF EXISTS test_conn_usable")
        cursor.close()
        conn.close()


def test_cache_disabled_with_ddl(db_kwargs):
    """
    Verifies that max_prepared_statements=0 (caching disabled) works
    correctly with DDL statements.

    Background:
        When max_prepared_statements=0, the cache initialization sets
        statement_dict to None (not an empty OrderedDict). All LRU
        management code is guarded by `if self.max_prepared_statements > 0`,
        so no cache operations should occur.

        handle_COMMAND_COMPLETE iterates pcache["ps"] and calls
        pcache["statement_dict"].clear(). With max_prepared_statements=0,
        statement_dict is None, so we need to ensure the DDL handler
        doesn't crash on None.clear().

    This test verifies:
        1. DDL + queries work without errors when caching is disabled
        2. statement_dict remains None throughout
    """
    db_kwargs["max_prepared_statements"] = 0
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        # DDL with caching disabled — handle_COMMAND_COMPLETE must not
        # crash when statement_dict is None.
        cursor.execute("CREATE TABLE IF NOT EXISTS test_no_cache (id INT)")

        # Execute many queries — none should be cached, no eviction
        # should occur, statement_dict should stay None.
        for i in range(10):
            cursor.execute(f"SELECT {i} AS no_cache_col")
            cursor.fetchall()

        # Verify statement_dict is still None (caching truly disabled).
        cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
        assert cache["statement_dict"] is None, "statement_dict should be None when caching is disabled"

    finally:
        cursor.execute("DROP TABLE IF EXISTS test_no_cache")
        cursor.close()
        conn.close()


def test_cache_reuse_after_ddl(db_kwargs):
    """
    Verifies that re-executing the same query after a DDL correctly
    creates a new prepared statement (cache miss) and the LRU tracking
    works properly for the re-added entry.

    Background:
        After DDL clears both ps and statement_dict, a previously-cached
        query becomes a cache miss. The driver must:
        1. Send a new PARSE message to create a fresh prepared statement
        2. Add the new entry to both ps and statement_dict
        3. Place it at the MRU (most recently used) position in statement_dict

        The cache["statement"] dict (which stores parsed SQL → make_args
        mappings) is NOT cleared by DDL, so the SQL parsing is still
        cached — only the server-side prepared statement is recreated.

    This test verifies the full round-trip: cache → DDL invalidation →
    cache miss → re-creation → correct LRU position.
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        reused_query = "SELECT 42 AS reuse_col"

        # Execute the query — gets cached in both ps and statement_dict.
        cursor.execute(reused_query)
        cursor.fetchall()

        # DDL clears both ps and statement_dict (with the fix).
        # The reused_query's prepared statement is now invalidated.
        cursor.execute("CREATE TABLE IF NOT EXISTS test_reuse (id INT)")

        # Re-execute the same query. This should be a cache miss in ps
        # (the key is gone), so the driver creates a new prepared
        # statement and adds it to both ps and statement_dict.
        cursor.execute(reused_query)
        result = cursor.fetchall()
        assert result[0][0] == 42

        # Verify ps and statement_dict are synchronized after re-add.
        cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
        assert len(cache["ps"]) == len(cache["statement_dict"]), (
            f"Cache desync after reuse: ps={len(cache['ps'])}, "
            f"statement_dict={len(cache['statement_dict'])}"
        )

        # Verify the reused query is at the MRU position (end of
        # the OrderedDict), confirming correct LRU tracking.
        mru_key = list(cache["statement_dict"].keys())[-1]
        assert mru_key[0] == reused_query, "Reused query should be most recently used"

    finally:
        cursor.execute("DROP TABLE IF EXISTS test_reuse")
        cursor.close()
        conn.close()


def test_cache_with_alter_table(db_kwargs):
    """
    Verifies ALTER TABLE triggers the same cache clearing as
    CREATE/DROP.

    Background:
        handle_COMMAND_COMPLETE checks for command in
        (b"ALTER", b"CREATE", b"DROP", b"ROLLBACK"). ALTER is the
        least commonly tested of these four, but it follows the exact
        same code path. This test ensures ALTER is not accidentally
        excluded from the cache clearing logic.

    Test flow:
        1. CREATE TABLE → clears cache (first DDL)
        2. Fill cache with 5 unique queries
        3. ALTER TABLE → clears cache again (second DDL)
        4. Fill cache with 6 unique queries (triggers eviction)
        5. Verify no crash and caches synchronized

    The eviction in step 4 would crash if ALTER didn't clear
    statement_dict, because stale keys from step 2 would still be
    present.
    """
    db_kwargs["max_prepared_statements"] = 5
    conn = redshift_connector.connect(**db_kwargs)
    cursor = conn.cursor()

    try:
        # First DDL: CREATE TABLE.
        cursor.execute("CREATE TABLE IF NOT EXISTS test_alter (id INT)")

        # Fill cache with 5 unique queries.
        for i in range(5):
            cursor.execute(f"SELECT {i} AS alter_col")
            cursor.fetchall()

        # Second DDL: ALTER TABLE. This must clear both ps and
        # statement_dict, just like CREATE/DROP/ROLLBACK.
        cursor.execute("ALTER TABLE test_alter ADD COLUMN name VARCHAR(100)")

        # Fill cache again with 6 unique queries (exceeds
        # max_prepared_statements=5, triggering 1 eviction).
        # Without the fix, eviction would pop a stale key from
        # the pre-ALTER statement_dict → KeyError.
        for i in range(6):
            cursor.execute(f"SELECT {i + 100} AS alter_col")
            cursor.fetchall()

        # Verify no crash and caches synchronized.
        cache = conn._caches[cursor.paramstyle][cursor.ps["pid"]]
        assert len(cache["ps"]) == len(cache["statement_dict"]), (
            f"Cache desync after ALTER: ps={len(cache['ps'])}, "
            f"statement_dict={len(cache['statement_dict'])}"
        )

    finally:
        cursor.execute("DROP TABLE IF EXISTS test_alter")
        cursor.close()
        conn.close()
