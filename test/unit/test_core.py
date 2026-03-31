import socket
from collections import OrderedDict
from os import getpid
from unittest.mock import MagicMock, Mock, call, patch

import pytest  # type: ignore

from redshift_connector.core import Connection
from redshift_connector.cursor import Cursor


@pytest.fixture
def connection():
    # Setup
    connection = Connection.__new__(Connection)

    # Setup prepared statement cache with some test data
    connection._caches = {
        "named": {
            "pid1": {
                "ps": {"stmt1": {"statement_name_bin": b"stmt1"}, "stmt2": {"statement_name_bin": b"stmt2"}},
                "statement_dict": OrderedDict([("stmt1", None), ("stmt2", None)]),
            }
        }
    }

    # Mock close_prepared_statement method to track calls
    connection.close_prepared_statement = Mock()
    connection._commands_with_count = (
        b"INSERT",
        b"DELETE",
        b"UPDATE",
        b"MOVE",
        b"FETCH",
        b"COPY",
        b"SELECT",
    )
    return connection


@pytest.fixture
def mock_socket_connection():
    """Fixture to provide a mock socket with basic connection behavior"""
    with patch("socket.socket") as mock_socket:
        mock_socket_instance = MagicMock()
        mock_socket.return_value = mock_socket_instance
        # Initial authentication response from server
        mock_socket_instance.recv.return_value = b"S"
        mock_file = MagicMock()
        mock_file.read.side_effect = [
            # Authentication request (R) message: 8 bytes total length, specifying auth type 0
            b"\x52\x00\x00\x00\x08",  # 'R' followed by message length
            b"\x00\x00\x00\x00",  # Auth type 0 (success)
            # ParameterStatus (S) message: setting client encoding
            b"\x53\x00\x00\x00\x1a",  # 'S' followed by message length (26 bytes)
            b"client_encoding\x00UTF8\x00",  # Parameter name and value, null-terminated
            # ReadyForQuery (Z) message: indicating idle state
            b"\x5a\x00\x00\x00\x05",  # 'Z' followed by message length (5 bytes)
            b"I",  # 'I' indicates idle state
        ]
        mock_socket_instance.makefile.return_value = mock_file
        yield mock_socket_instance


@pytest.mark.parametrize("command_status", [b"ALTER\x00", b"CREATE\x00", b"DROP\x00", b"ROLLBACK\x00"])
def test_handle_command_complete_trigger_prepared_statement_cache_clean_up(command_status, connection):
    """
    Test that DDL commands (ALTER, CREATE, DROP, ROLLBACK) trigger the prepared statement cache cleanup.
    This test verifies that when these commands are executed, all prepared statements are closed and
    removed from the cache.

    Args:
        command_status (bytes): The command status message from the server
        connection (Connection): Fixture providing a mock connection with prepared statements cache
    """

    # Verify non-empty cache precondition
    initial_stmt_count = sum(
        len(pid_cache["ps"])
        for paramstyle_cache in connection._caches.values()
        for pid_cache in paramstyle_cache.values()
    )
    assert initial_stmt_count > 0, "Test requires non-empty initial cache"

    cursor = Mock(spec=Cursor)
    connection.handle_COMMAND_COMPLETE(command_status, cursor)

    # Verify that close_prepared_statement was called for each cached statement
    # Should be called twice as we have two statements in our test cache
    assert connection.close_prepared_statement.call_count == 2

    # Verify close_prepared_statement was called with correct statement names
    connection.close_prepared_statement.assert_any_call(b"stmt1")
    connection.close_prepared_statement.assert_any_call(b"stmt2")

    # Verify cache was cleared
    for scache in connection._caches.values():
        for pcache in scache.values():
            assert len(pcache["ps"]) == 0, "Cache should be empty after ALTER/CREATE/DROP/ROLLBACK command"


@pytest.mark.parametrize(
    "command_status",
    [
        b"SELECT 1\x00",
        b"INSERT 0 1\x00",
        b"UPDATE 1\x00",
        b"DELETE 1\x00",
        b"MOVE 1\x00",
        b"FETCH 1\x00",
        b"COPY 1\x00",
        b"BEGIN\x00",
        b"COMMIT\x00",
    ],
)
def test_handle_command_complete_no_cache_cleanup(command_status, connection):
    """
    Test that non-DDL commands (SELECT, INSERT, UPDATE, etc.) do not trigger
    prepared statement cache cleanup. The cache should remain unchanged after
    executing these commands.

    Args:
        command_status (bytes): The command status message from the server
        connection (Connection): Fixture providing a mock connection with prepared statements cache
    """

    # Create deep copy of initial cache state for later comparison
    initial_cache = {
        paramstyle: {
            pid: {"ps": {stmt: ps.copy() for stmt, ps in pid_cache["ps"].items()}}
            for pid, pid_cache in paramstyle_cache.items()
        }
        for paramstyle, paramstyle_cache in connection._caches.items()
    }

    # Verify non-empty cache precondition
    initial_stmt_count = sum(len(pcache["ps"]) for scache in connection._caches.values() for pcache in scache.values())
    assert initial_stmt_count > 0, "Test requires non-empty initial cache"

    cursor = Mock(spec=Cursor)
    cursor._row_count = 1
    connection.handle_COMMAND_COMPLETE(command_status, cursor)

    # Verify that close_prepared_statement was not called
    # Non-DDL commands should not trigger cache cleanup
    connection.close_prepared_statement.assert_not_called()

    # Verify cache remains unchanged after command execution
    for schema, scache in connection._caches.items():
        for pid, pcache in scache.items():
            assert (
                pcache["ps"] == initial_cache[schema][pid]["ps"]
            ), f"Cache should remain unchanged after {command_status.decode().strip()} command"
            assert (
                len(pcache["ps"]) == 2
            ), f"Cache should still contain both statements after {command_status.decode().strip()} command"

    # Verify specific statements still exist in cache with correct binary names
    assert b"stmt1" in connection._caches["named"]["pid1"]["ps"]["stmt1"]["statement_name_bin"]
    assert b"stmt2" in connection._caches["named"]["pid1"]["ps"]["stmt2"]["statement_name_bin"]


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "max_prepared_statements_zero",
            "max_prepared_statements": 0,
            "queries": ["SELECT 1", "SELECT 2"],
            "expected_close_calls": 0,
        },
        {
            "name": "max_prepared_statements_default",
            "max_prepared_statements": 1000,
            "queries": ["SELECT 1", "SELECT 2"],
            "expected_close_calls": 0,
        },
        {
            "name": "max_prepared_statements_limit_1",
            "max_prepared_statements": 2,
            "queries": ["SELECT 1", "SELECT 2", "SELECT 3"],
            "expected_close_calls": 1,
        },
        {
            "name": "max_prepared_statements_limit_2",
            "max_prepared_statements": 2,
            "queries": ["SELECT 1", "SELECT 2"],
            "expected_close_calls": 0,
        },
    ],
)
def test_prepared_statement_cache_behavior(mocker, test_case):
    """
    Test prepared statement cache management in execute() with different configurations.
    :type mocker: object
    :param test_case: Dictionary containing test parameters
    """
    # Setup Connection instance
    mock_connection = Connection.__new__(Connection)
    mock_connection.max_prepared_statements = test_case["max_prepared_statements"]
    mock_connection.merge_socket_read = True
    mock_connection._caches = {}
    mock_connection._send_message = mocker.Mock()
    mock_connection._write = mocker.Mock()
    mock_connection._flush = mocker.Mock()
    mock_connection.handle_messages = mocker.Mock()
    mock_connection.handle_messages_merge_socket_read = mocker.Mock()
    mock_connection.close_prepared_statement = mocker.Mock()

    # Mock cursor
    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = "named"

    # Execute multiple statements
    for query in test_case["queries"]:
        mock_connection.execute(mock_cursor, query, None)

    # Verify close_prepared_statement was called for each execution
    assert mock_connection.close_prepared_statement.call_count == test_case["expected_close_calls"]


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "statement_reuse",
            "queries": ["SELECT 1", "SELECT 2", "SELECT 1", "SELECT 3"],
            "expected_in_cache": ["SELECT 1", "SELECT 3"],
            "expected_not_in_cache": ["SELECT 2"],
            "expected_close_calls": 1,
        },
        {
            "name": "lru_order",
            "queries": ["SELECT 1", "SELECT 2", "SELECT 1", "SELECT 2", "SELECT 3"],
            "expected_in_cache": ["SELECT 2", "SELECT 3"],
            "expected_not_in_cache": ["SELECT 1"],
            "expected_close_calls": 1,
        },
    ],
)
def test_prepared_statement_lru_behavior(mocker, test_case):
    """Test LRU behavior of prepared statement cache."""
    from os import getpid

    mock_connection = Connection.__new__(Connection)
    mock_connection.max_prepared_statements = 2  # Set to 2 for LRU testing
    mock_connection.merge_socket_read = True
    mock_connection._caches = {}
    mock_connection._send_message = mocker.Mock()
    mock_connection._write = mocker.Mock()
    mock_connection._flush = mocker.Mock()
    mock_connection.handle_messages = mocker.Mock()
    mock_connection.handle_messages_merge_socket_read = mocker.Mock()
    mock_connection.close_prepared_statement = mocker.Mock()

    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = "named"

    for query in test_case["queries"]:
        mock_connection.execute(mock_cursor, query, None)

    assert mock_connection.close_prepared_statement.call_count == test_case["expected_close_calls"]

    # Verify cache contents
    pid = getpid()
    cache = mock_connection._caches["named"][pid]
    for query in test_case["expected_in_cache"]:
        assert any(key[0] == query for key in cache["statement_dict"])
    for query in test_case["expected_not_in_cache"]:
        assert not any(key[0] == query for key in cache["statement_dict"])


@pytest.mark.parametrize(
    "test_case",
    [
        {"max_prepared_statements": 0, "statement_name": "stmt1", "expected_statement_name": ""},
        {"max_prepared_statements": 1000, "statement_name": "stmt1", "expected_statement_name": "stmt1"},
    ],
)
def test_get_statement_name_bin(test_case):
    """
    Test the get_statement_name_bin method of Connection class.

    This test verifies that the method correctly handles statement names based on
    the max_prepared_statements configuration:
     - When max_prepared_statements is 0 (disabled), it should return an empty string as bytes
     - When max_prepared_statements > 0 (enabled), it should return the original statement name as bytes

    The test asserts that the binary output matches the expected statement name
    encoded in ASCII with a null terminator.
    """
    # Setup Connection instance
    mock_connection = Connection.__new__(Connection)
    mock_connection.max_prepared_statements = test_case["max_prepared_statements"]
    assert (
        mock_connection.get_statement_name_bin(test_case["statement_name"])
        == test_case["expected_statement_name"].encode("ascii") + b"\x00"
    )


@pytest.mark.parametrize(
    "max_prepared_statements,expected_result", [(0, 0), (10, 10), (-1, 1000)]  # Invalid input results in default value
)
def test_get_max_prepared_statement(max_prepared_statements, expected_result):
    """
    Test the get_max_prepared_statement method of the Connection class.

    This test verifies that the method correctly returns the max_prepared_statements
    value even when the user inputs an invalid number.
    """
    # Setup Connection instance
    mock_connection = Connection.__new__(Connection)
    assert mock_connection.get_max_prepared_statement(max_prepared_statements) == expected_result


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "system_defaults",
            "params": {},
            "expected": {"keepalive": True, "check_values": False},  # Keepalive defaults to True
        },
        {
            "name": "custom_values",
            "params": {
                "tcp_keepalive": True,
                "tcp_keepalive_idle": 300,
                "tcp_keepalive_interval": 60,
                "tcp_keepalive_count": 5,
            },
            "expected": {
                "keepalive": True,
                "check_values": True,
                "keepalive_idle": 300,
                "keepalive_interval": 60,
                "keepalive_count": 5,
            },
        },
        {
            "name": "keepalive_enabled_with_defaults",
            "params": {"tcp_keepalive": True},
            "expected": {
                "keepalive": True,
                "check_values": False,  # Don't check values as they should be system defaults
            },
        },
        {
            "name": "keepalive_disabled",
            "params": {"tcp_keepalive": False},
            "expected": {"keepalive": False, "check_values": False},
        },
    ],
)
def test_tcp_keepalive_configuration(test_case: dict):
    """
    Test TCP keepalive configuration with different settings.
    TCP keepalive is enabled by default.
    """
    with patch("socket.socket") as mock_socket:
        # Mock socket behavior
        mock_socket_instance = MagicMock()
        mock_socket.return_value = mock_socket_instance
        mock_socket_instance.recv.return_value = b"S"
        mock_file = MagicMock()
        mock_file.read.side_effect = [
            b"\x52\x00\x00\x00\x08",
            b"\x00\x00\x00\x00",
            b"\x53\x00\x00\x00\x1a",
            b"client_encoding\x00UTF8\x00",
            b"\x5a\x00\x00\x00\x05",
            b"I",
        ]
        mock_socket_instance.makefile.return_value = mock_file

        # Create connection with test values
        Connection(
            user="test_user",
            password="test_password",
            database="test_db",
            ssl=False,
            host="localhost",
            **test_case["params"],
        )

        if test_case["expected"]["keepalive"]:
            # Verify keepalive was enabled
            keepalive_call = call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            assert (
                keepalive_call in mock_socket_instance.setsockopt.call_args_list
            ), "Expected SO_KEEPALIVE to be enabled"

            if test_case["expected"]["check_values"]:
                # Only verify specific values if they were explicitly set
                expected_calls = []

                # Add platform-specific idle time call
                if hasattr(socket, "TCP_KEEPIDLE"):
                    expected_calls.append(
                        call(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, test_case["expected"]["keepalive_idle"])
                    )
                elif hasattr(socket, "TCP_KEEPALIVE"):
                    expected_calls.append(
                        call(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, test_case["expected"]["keepalive_idle"])
                    )

                # Add other keepalive calls if supported
                if hasattr(socket, "TCP_KEEPINTVL"):
                    expected_calls.append(
                        call(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, test_case["expected"]["keepalive_interval"])
                    )
                if hasattr(socket, "TCP_KEEPCNT"):
                    expected_calls.append(
                        call(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, test_case["expected"]["keepalive_count"])
                    )

                for expected_call in expected_calls:
                    assert (
                        expected_call in mock_socket_instance.setsockopt.call_args_list
                    ), f"Expected call {expected_call} not found in {mock_socket_instance.setsockopt.call_args_list}"
        else:
            # When explicitly disabled, verify no keepalive calls were made
            keepalive_calls = [c for c in mock_socket_instance.setsockopt.call_args_list if socket.SO_KEEPALIVE in c[0]]
            assert len(keepalive_calls) == 0, f"Expected no keepalive calls but found {keepalive_calls}"


@pytest.mark.parametrize(
    "platform_config",
    [
        {
            "name": "linux_platform",
            "socket_attributes": {
                "TCP_KEEPIDLE": True,
                "TCP_KEEPALIVE": False,
                "TCP_KEEPINTVL": True,
                "TCP_KEEPCNT": True,
            },
        },
        {
            "name": "macos_platform",
            "socket_attributes": {
                "TCP_KEEPIDLE": False,
                "TCP_KEEPALIVE": True,
                "TCP_KEEPINTVL": True,
                "TCP_KEEPCNT": True,
            },
        },
        {
            "name": "limited_platform",
            "socket_attributes": {
                "TCP_KEEPALIVE": False,
                "TCP_KEEPIDLE": False,
                "TCP_KEEPINTVL": False,
                "TCP_KEEPCNT": False,
            },
        },
    ],
)
def test_tcp_keepalive_platform_support(platform_config: dict, mock_socket_connection):
    """
    Test TCP keepalive configuration across different platforms with varying socket support.
    Only tests that keepalive can be enabled/disabled as other values use system defaults.
    """
    # Store original socket attributes
    original_attrs = {}
    for attr in platform_config["socket_attributes"]:
        original_attrs[attr] = getattr(socket, attr, None)

    try:
        # Mock platform-specific socket attributes
        for attr, has_attr in platform_config["socket_attributes"].items():
            if has_attr:
                setattr(socket, attr, 1)
            elif hasattr(socket, attr):
                delattr(socket, attr)

        # Create connection with keepalive enabled
        Connection(
            user="test_user",
            password="test_password",
            database="test_db",
            ssl=False,
            host="localhost",
            tcp_keepalive=True,
        )

        # Verify only that keepalive was enabled
        keepalive_call = call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        assert keepalive_call in mock_socket_connection.setsockopt.call_args_list

    finally:
        # Restore original socket attributes
        for attr, value in original_attrs.items():
            if value is not None:
                setattr(socket, attr, value)
            elif hasattr(socket, attr):
                delattr(socket, attr)


# ============================================================================
# Bug Condition Exploration Tests: Cache Desync After DDL/ROLLBACK
# ============================================================================
#
# Two tests cover the two layers of defense:
# 1. test_handle_command_complete_clears_both_caches: Tests the primary fix
#    in handle_COMMAND_COMPLETE — that both ps and statement_dict are cleared
#    together when a DDL/ROLLBACK command completes. Parametrized by DDL
#    command type since each goes through the same code path.
# 2. test_eviction_skips_stale_keys_in_statement_dict: Tests the defense-in-
#    depth eviction guard — that if statement_dict somehow has stale keys
#    (e.g. from an unexpected desync), eviction skips them instead of crashing.
# ============================================================================


@pytest.mark.parametrize("ddl_command", [b"ALTER\x00", b"CREATE\x00", b"DROP\x00", b"ROLLBACK\x00"])
def test_handle_command_complete_clears_both_caches(mocker, ddl_command):
    """
    Tests the primary fix: handle_COMMAND_COMPLETE clears both cache["ps"]
    and cache["statement_dict"] when a DDL/ROLLBACK command completes.

    This routes through handle_COMMAND_COMPLETE with each DDL command type
    to verify the root-cause fix on line 2068 of core.py. After the command
    completes, both ps and statement_dict must be empty. Then refilling the
    cache and triggering eviction must work without KeyError.

    EXPECTED ON UNFIXED CODE: statement_dict retains stale keys after DDL,
    leading to KeyError during eviction.
    EXPECTED ON FIXED CODE: Both caches cleared, eviction works correctly.
    """
    pid = getpid()
    paramstyle = "named"

    conn = Connection.__new__(Connection)
    conn.max_prepared_statements = 3
    conn.merge_socket_read = True
    conn._caches = {}
    conn._commands_with_count = (b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH", b"COPY", b"SELECT")
    conn._send_message = mocker.Mock()
    conn._write = mocker.Mock()
    conn._flush = mocker.Mock()
    conn.handle_messages = mocker.Mock()
    conn.handle_messages_merge_socket_read = mocker.Mock()
    conn.close_prepared_statement = mocker.Mock()

    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    # Step 1: Fill cache to max_prepared_statements
    for i in range(3):
        conn.execute(mock_cursor, f"SELECT initial_{i}", None)

    cache = conn._caches[paramstyle][pid]
    assert len(cache["ps"]) == 3
    assert len(cache["statement_dict"]) == 3

    # Step 2: Route through handle_COMMAND_COMPLETE with the DDL command.
    # This is the real code path — not a manual desync simulation.
    conn.handle_COMMAND_COMPLETE(ddl_command, mock_cursor)

    # Verify the primary fix: both ps and statement_dict must be cleared
    assert len(cache["ps"]) == 0, (
        f"ps should be empty after {ddl_command!r}, but has {len(cache['ps'])} entries"
    )
    assert len(cache["statement_dict"]) == 0, (
        f"statement_dict should be empty after {ddl_command!r}, "
        f"but has {len(cache['statement_dict'])} entries (stale keys)"
    )

    # Step 3: Refill cache and trigger eviction — must not crash
    for i in range(3):
        conn.execute(mock_cursor, f"SELECT refill_{i}", None)

    conn.execute(mock_cursor, "SELECT trigger_eviction", None)

    # Verify caches remain synchronized after eviction
    assert len(cache["ps"]) == len(cache["statement_dict"]), (
        f"Cache desync after eviction: ps={len(cache['ps'])}, "
        f"statement_dict={len(cache['statement_dict'])}"
    )
    assert set(cache["ps"].keys()) == set(cache["statement_dict"].keys()), (
        f"Key mismatch: "
        f"in statement_dict but not ps: {set(cache['statement_dict'].keys()) - set(cache['ps'].keys())}"
    )


def test_consistency_repair_rebuilds_statement_dict_after_desync(mocker):
    """
    Tests the consistency-repair block: when len(ps) != len(statement_dict),
    statement_dict is cleared and rebuilt from ps, removing stale keys.

    This manually injects a desync state (ps cleared, statement_dict has
    stale keys) to exercise the consistency-repair path. After repair,
    eviction and new entry insertion succeed without error.

    EXPECTED ON UNFIXED CODE: KeyError during eviction (stale keys persist)
    EXPECTED ON FIXED CODE: Consistency-repair removes stale keys, eviction succeeds
    """
    pid = getpid()
    paramstyle = "named"

    conn = Connection.__new__(Connection)
    conn.max_prepared_statements = 3
    conn.merge_socket_read = True
    conn._caches = {}
    conn._send_message = mocker.Mock()
    conn._write = mocker.Mock()
    conn._flush = mocker.Mock()
    conn.handle_messages = mocker.Mock()
    conn.handle_messages_merge_socket_read = mocker.Mock()
    conn.close_prepared_statement = mocker.Mock()

    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    # Fill cache to max
    for i in range(3):
        conn.execute(mock_cursor, f"SELECT initial_{i}", None)

    cache = conn._caches[paramstyle][pid]
    stale_keys = list(cache["statement_dict"].keys())

    # Manually inject desync: clear ps but leave statement_dict with stale keys.
    # This simulates any code path that might desync the two structures.
    for ps_entry in cache["ps"].values():
        conn.close_prepared_statement(ps_entry["statement_name_bin"])
    cache["ps"].clear()

    assert len(cache["ps"]) == 0
    assert len(cache["statement_dict"]) == 3, "statement_dict should still have stale keys"

    # Refill cache — consistency-repair will rebuild statement_dict from ps,
    # but if it only adds (not removes), stale keys persist
    for i in range(3):
        conn.execute(mock_cursor, f"SELECT refill_{i}", None)

    # Trigger eviction — the while-loop guard must skip stale keys
    try:
        conn.execute(mock_cursor, "SELECT trigger_eviction", None)
    except KeyError as e:
        pytest.fail(
            f"KeyError during eviction: {e}. "
            f"Stale keys: {stale_keys}. "
            f"The eviction guard should have skipped stale keys."
        )

    # Verify caches are synchronized
    assert len(cache["ps"]) == len(cache["statement_dict"]), (
        f"Cache desync: ps={len(cache['ps'])}, statement_dict={len(cache['statement_dict'])}"
    )
    assert set(cache["ps"].keys()) == set(cache["statement_dict"].keys())


def test_eviction_while_loop_skips_stale_keys(mocker):
    """
    Tests the defense-in-depth while-loop eviction guard directly.

    The consistency-repair block only triggers when len(ps) != len(statement_dict).
    To exercise the while-loop guard, we inject a desync where both structures
    have the SAME length but statement_dict contains stale keys not in ps.
    This bypasses consistency-repair and forces the eviction loop to encounter
    and skip stale keys.

    Setup: ps has 3 valid keys, statement_dict has 2 stale keys + 1 valid key
    (same length=3, but different keys). When eviction runs, the while-loop
    must skip the 2 stale keys and evict the 1 valid key.
    """
    pid = getpid()
    paramstyle = "named"

    conn = Connection.__new__(Connection)
    conn.max_prepared_statements = 3
    conn.merge_socket_read = True
    conn._caches = {}
    conn._send_message = mocker.Mock()
    conn._write = mocker.Mock()
    conn._flush = mocker.Mock()
    conn.handle_messages = mocker.Mock()
    conn.handle_messages_merge_socket_read = mocker.Mock()
    conn.close_prepared_statement = mocker.Mock()

    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    # Fill cache to max with 3 entries
    for i in range(3):
        conn.execute(mock_cursor, f"SELECT valid_{i}", None)

    cache = conn._caches[paramstyle][pid]
    assert len(cache["ps"]) == 3
    assert len(cache["statement_dict"]) == 3

    # Manually inject desync: replace 2 of the 3 keys in statement_dict with
    # stale keys that don't exist in ps, keeping len(statement_dict) == 3.
    # This bypasses the consistency-repair check (lengths match) and forces
    # the while-loop to handle stale keys during eviction.
    valid_keys = list(cache["statement_dict"].keys())
    cache["statement_dict"].clear()
    # Add 2 stale keys (oldest, will be popped first)
    stale_key_1 = ("SELECT stale_0", ())
    stale_key_2 = ("SELECT stale_1", ())
    cache["statement_dict"][stale_key_1] = None
    cache["statement_dict"][stale_key_2] = None
    # Keep 1 valid key (the last one from ps)
    cache["statement_dict"][valid_keys[2]] = None

    assert len(cache["ps"]) == 3
    assert len(cache["statement_dict"]) == 3  # Same length — consistency-repair won't trigger

    # Execute a new query to trigger eviction. The while-loop should:
    # 1. Pop stale_key_1 — not in ps, skip
    # 2. Pop stale_key_2 — not in ps, skip
    # 3. Pop valid_keys[2] — in ps, evict it, break
    try:
        conn.execute(mock_cursor, "SELECT trigger_eviction", None)
    except KeyError as e:
        pytest.fail(
            f"KeyError during eviction with same-length desync: {e}. "
            f"Stale keys: [{stale_key_1}, {stale_key_2}]. "
            f"The while-loop guard should have skipped them."
        )

    # Verify the while-loop handled eviction correctly:
    # - No KeyError was raised (the try/except above would have caught it)
    # - valid_keys[2] was evicted from ps (the only valid key in statement_dict)
    # - The new query was added to both ps and statement_dict
    # - ps has 3 entries: valid_0, valid_1, trigger_eviction
    # - statement_dict has 1 entry: trigger_eviction (the 2 stale keys were
    #   discarded and valid_keys[2] was evicted, leaving only the new entry)
    #
    # Note: ps and statement_dict are NOT fully in sync here because we
    # injected a same-length desync. The next execute() will detect
    # len(ps) != len(statement_dict) and trigger consistency-repair.
    assert valid_keys[2] not in cache["ps"], "The valid key used for eviction should be gone"
    assert ("SELECT trigger_eviction", ()) in cache["ps"], "New query should be in ps"
    assert ("SELECT trigger_eviction", ()) in cache["statement_dict"], "New query should be in statement_dict"
    assert len(cache["ps"]) == 3, f"ps should have 3 entries, got {len(cache['ps'])}"
    # statement_dict only has the new entry — the 2 stale keys were discarded
    # and valid_keys[2] was evicted. valid_0 and valid_1 were never in
    # statement_dict (we replaced them with stale keys in the setup).
    assert len(cache["statement_dict"]) == 1, (
        f"statement_dict should have 1 entry (only the new query), got {len(cache['statement_dict'])}"
    )

    # Verify the next execute triggers consistency-repair and restores full sync
    conn.execute(mock_cursor, "SELECT after_repair", None)
    assert len(cache["ps"]) == len(cache["statement_dict"]), (
        f"After consistency-repair: ps={len(cache['ps'])}, statement_dict={len(cache['statement_dict'])}"
    )
    assert set(cache["ps"].keys()) == set(cache["statement_dict"].keys())


# ============================================================================
# Preservation Property Tests: Non-DDL LRU Cache Behavior Unchanged
# ============================================================================
#
# These tests verify that non-DDL query sequences produce correct caching,
# eviction, and reuse behavior. They must PASS on UNFIXED code (confirming
# baseline behavior) and continue to PASS after the fix (no regressions).
# ============================================================================


def _make_connection_for_cache_test(mocker, max_prepared_statements):
    """Helper to create a Connection mock suitable for cache behavior testing."""
    conn = Connection.__new__(Connection)
    conn.max_prepared_statements = max_prepared_statements
    conn.merge_socket_read = True
    conn._caches = {}
    conn._send_message = mocker.Mock()
    conn._write = mocker.Mock()
    conn._flush = mocker.Mock()
    conn.handle_messages = mocker.Mock()
    conn.handle_messages_merge_socket_read = mocker.Mock()
    conn.close_prepared_statement = mocker.Mock()
    return conn


@pytest.mark.parametrize(
    "max_ps,queries",
    [
        # Small cache, unique queries that trigger multiple evictions
        (2, ["SELECT a", "SELECT b", "SELECT c", "SELECT d", "SELECT e"]),
        # Cache exactly fits all queries — no eviction
        (5, ["SELECT a", "SELECT b", "SELECT c", "SELECT d", "SELECT e"]),
        # Single-slot cache — every new query evicts the previous one
        (1, ["SELECT a", "SELECT b", "SELECT c"]),
        # Moderate cache with more queries than capacity
        (3, ["SELECT q1", "SELECT q2", "SELECT q3", "SELECT q4", "SELECT q5", "SELECT q6"]),
    ],
    ids=["small_cache_evictions", "exact_fit_no_eviction", "single_slot_cache", "moderate_overflow"],
)
def test_preservation_cache_sync_after_every_operation(mocker, max_ps, queries):
    """
    Preservation Property: For all non-DDL query sequences with max_prepared_statements > 0,
    len(cache["ps"]) == len(cache["statement_dict"]) and
    set(cache["ps"].keys()) == set(cache["statement_dict"].keys()) after every operation.

    Also verifies cache size never exceeds max_prepared_statements.
    """
    pid = getpid()
    paramstyle = "named"

    conn = _make_connection_for_cache_test(mocker, max_ps)
    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    for query in queries:
        conn.execute(mock_cursor, query, None)

        cache = conn._caches[paramstyle][pid]

        # Property: ps and statement_dict have the same length
        assert len(cache["ps"]) == len(cache["statement_dict"]), (
            f"After executing '{query}': ps has {len(cache['ps'])} entries "
            f"but statement_dict has {len(cache['statement_dict'])} entries"
        )

        # Property: ps and statement_dict have the same keys
        assert set(cache["ps"].keys()) == set(cache["statement_dict"].keys()), (
            f"After executing '{query}': key mismatch — "
            f"in statement_dict only: {set(cache['statement_dict'].keys()) - set(cache['ps'].keys())}; "
            f"in ps only: {set(cache['ps'].keys()) - set(cache['statement_dict'].keys())}"
        )

        # Property: cache size never exceeds max_prepared_statements
        assert len(cache["ps"]) <= max_ps, (
            f"After executing '{query}': cache size {len(cache['ps'])} "
            f"exceeds max_prepared_statements={max_ps}"
        )


@pytest.mark.parametrize(
    "max_ps,queries",
    [
        # Re-access first query after adding second — first should be MRU
        (3, ["SELECT x", "SELECT y", "SELECT x"]),
        # Repeated access pattern — last accessed should be at MRU position
        (3, ["SELECT a", "SELECT b", "SELECT c", "SELECT a", "SELECT b"]),
        # Single repeated query — always MRU
        (2, ["SELECT only", "SELECT other", "SELECT only"]),
    ],
    ids=["reaccess_moves_to_mru", "repeated_access_pattern", "single_repeat"],
)
def test_preservation_cache_hit_moves_to_mru(mocker, max_ps, queries):
    """
    Preservation Property: Repeated queries are cache hits (no new PARSE sent),
    and the key moves to MRU position in statement_dict.
    """
    pid = getpid()
    paramstyle = "named"

    conn = _make_connection_for_cache_test(mocker, max_ps)
    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    def count_parse_calls():
        """Count how many times _send_message was called with PARSE (b'P') as first arg."""
        return sum(
            1 for c in conn._send_message.call_args_list
            if c[0][0] == b"P"
        )

    seen_queries = set()
    for query in queries:
        parse_count_before = count_parse_calls()
        conn.execute(mock_cursor, query, None)
        parse_count_after = count_parse_calls()

        cache = conn._caches[paramstyle][pid]
        key = (query, ())  # no params → empty tuple

        if query in seen_queries:
            # Cache hit: no new PARSE should have been sent
            assert parse_count_after == parse_count_before, (
                f"Cache hit for '{query}' should not send new PARSE messages, "
                f"but {parse_count_after - parse_count_before} new PARSE calls were made"
            )

            # The key should be at MRU position (last in OrderedDict)
            last_key = list(cache["statement_dict"].keys())[-1]
            assert last_key == key, (
                f"After cache hit for '{query}', expected key at MRU position (last), "
                f"but last key is {last_key[0]}"
            )
        else:
            seen_queries.add(query)

        # Invariant: caches stay synchronized
        assert len(cache["ps"]) == len(cache["statement_dict"])
        assert set(cache["ps"].keys()) == set(cache["statement_dict"].keys())


@pytest.mark.parametrize(
    "max_ps,queries",
    [
        # 3 unique queries in a cache of 2 — first should be evicted
        (2, ["SELECT first", "SELECT second", "SELECT third"]),
        # 4 unique queries in a cache of 2 — first two should be evicted
        (2, ["SELECT a", "SELECT b", "SELECT c", "SELECT d"]),
        # Cache hit reorders LRU — eviction should skip the reaccessed one
        (2, ["SELECT a", "SELECT b", "SELECT a", "SELECT c"]),
    ],
    ids=["single_eviction", "double_eviction", "reaccess_changes_eviction_order"],
)
def test_preservation_lru_eviction_order(mocker, max_ps, queries):
    """
    Preservation Property: When cache is full, the least recently used entry
    is evicted and close_prepared_statement is called. Eviction follows LRU order.
    """
    pid = getpid()
    paramstyle = "named"

    conn = _make_connection_for_cache_test(mocker, max_ps)
    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    for query in queries:
        conn.execute(mock_cursor, query, None)

    cache = conn._caches[paramstyle][pid]

    # Verify eviction happened (close_prepared_statement was called)
    expected_evictions = max(0, len(set(queries)) - max_ps)
    # Note: close_prepared_statement count may differ from expected_evictions
    # because cache hits don't trigger eviction. Count unique queries seen in order.
    unique_in_order = []
    seen = set()
    for q in queries:
        if q not in seen:
            unique_in_order.append(q)
            seen.add(q)
    actual_evictions = max(0, len(unique_in_order) - max_ps)
    assert conn.close_prepared_statement.call_count == actual_evictions, (
        f"Expected {actual_evictions} evictions but got "
        f"{conn.close_prepared_statement.call_count}"
    )

    # Verify final cache size
    assert len(cache["ps"]) == min(len(unique_in_order), max_ps)
    assert len(cache["ps"]) == len(cache["statement_dict"])

    # Verify the last max_ps unique queries (by LRU order) are in cache
    # Build expected cache contents by simulating LRU
    lru = OrderedDict()
    for q in queries:
        key = (q, ())
        if key in lru:
            lru.move_to_end(key)
        else:
            if len(lru) >= max_ps:
                lru.popitem(last=False)
            lru[key] = None

    assert set(cache["statement_dict"].keys()) == set(lru.keys()), (
        f"Cache contents don't match expected LRU state. "
        f"Expected: {set(k[0] for k in lru.keys())}, "
        f"Got: {set(k[0] for k in cache['statement_dict'].keys())}"
    )


@pytest.mark.parametrize(
    "queries",
    [
        ["SELECT 1", "SELECT 2", "SELECT 3"],
        ["SELECT a", "SELECT a", "SELECT b"],
        ["SELECT only"],
    ],
    ids=["multiple_unique", "with_repeat", "single_query"],
)
def test_preservation_max_prepared_statements_zero(mocker, queries):
    """
    Preservation Property: With max_prepared_statements=0, statement_dict is not
    used (is None) and cache remains empty (ps has no entries managed by LRU).
    """
    pid = getpid()
    paramstyle = "named"

    conn = _make_connection_for_cache_test(mocker, max_prepared_statements=0)
    mock_cursor = mocker.Mock()
    mock_cursor.paramstyle = paramstyle

    for query in queries:
        conn.execute(mock_cursor, query, None)

    cache = conn._caches[paramstyle][pid]

    # statement_dict should be None when max_prepared_statements=0
    assert cache["statement_dict"] is None, (
        f"statement_dict should be None when max_prepared_statements=0, "
        f"but got {type(cache['statement_dict'])}"
    )

    # No eviction should ever be called
    conn.close_prepared_statement.assert_not_called()

    # ps still stores statements (they're just not LRU-managed)
    # but no LRU cache management occurs
