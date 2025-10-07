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
    db_kwargs['max_prepared_statements'] = 5
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
        cache = conn._caches[cursor.paramstyle][cursor.ps['pid']]
        statement_dict = cache['statement_dict']

        # Basic cache size verification
        assert len(cache['ps']) == 5, f"Cache size should be 5, but was {len(cache['ps'])}"
        assert len(statement_dict) == 5, f"Statement dict size should be 5, but was {len(statement_dict)}"

        # Verify the most recent statements are in the queue
        cached_statements = [key[0] for key in statement_dict.keys()]
        last_five_statements = executed_statements[-5:]
        assert all(stmt in cached_statements for stmt in last_five_statements), \
            "Last 5 statements should be in cache"

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
        assert len(cache['ps']) == 5, f"Cache size should still be 5, but was {len(cache['ps'])}"
        assert list(statement_dict.keys())[-1][0] == new_stmt, "New statement should be most recent"

        # Verify LRU eviction - the least recently used statement should be gone
        current_statements = [key[0] for key in statement_dict]
        lru_statement = statements_before_new[0]  # First statement in queue before adding new one
        assert lru_statement not in current_statements, \
            f"Least recently used statement should have been evicted: {lru_statement}"

    finally:
        cursor.close()
        conn.close()
