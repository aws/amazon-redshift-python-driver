from unittest.mock import Mock, patch
from redshift_connector.core import Connection
from redshift_connector.cursor import Cursor

import pytest  # type: ignore

@pytest.fixture
def connection():
    # Setup
    connection = Connection.__new__(Connection)

    # Setup prepared statement cache with some test data
    connection._caches = {
        'named': {
            'pid1': {
                'ps': {
                    'stmt1': {'statement_name_bin': b'stmt1'},
                    'stmt2': {'statement_name_bin': b'stmt2'}
                }
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

@pytest.mark.parametrize("command_status", [
    b'ALTER\x00',
    b'CREATE\x00',
    b'DROP\x00',
    b'ROLLBACK\x00'
])
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
        len(pid_cache['ps'])
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
    connection.close_prepared_statement.assert_any_call(b'stmt1')
    connection.close_prepared_statement.assert_any_call(b'stmt2')

    # Verify cache was cleared
    for scache in connection._caches.values():
        for pcache in scache.values():
            assert len(pcache['ps']) == 0, "Cache should be empty after ALTER/CREATE/DROP/ROLLBACK command"

@pytest.mark.parametrize("command_status", [
    b'SELECT 1\x00',
    b'INSERT 0 1\x00',
    b'UPDATE 1\x00',
    b'DELETE 1\x00',
    b'MOVE 1\x00',
    b'FETCH 1\x00',
    b'COPY 1\x00',
    b'BEGIN\x00',
    b'COMMIT\x00'
])
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
            pid: {
                'ps': {stmt: ps.copy() for stmt, ps in pid_cache['ps'].items()}
            } for pid, pid_cache in paramstyle_cache.items()
        } for paramstyle, paramstyle_cache in connection._caches.items()
    }

    # Verify non-empty cache precondition
    initial_stmt_count = sum(
        len(pcache['ps'])
        for scache in connection._caches.values()
        for pcache in scache.values()
    )
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
            assert pcache['ps'] == initial_cache[schema][pid]['ps'], \
                f"Cache should remain unchanged after {command_status.decode().strip()} command"
            assert len(pcache['ps']) == 2, \
                f"Cache should still contain both statements after {command_status.decode().strip()} command"

    # Verify specific statements still exist in cache with correct binary names
    assert b'stmt1' in connection._caches['named']['pid1']['ps']['stmt1']['statement_name_bin']
    assert b'stmt2' in connection._caches['named']['pid1']['ps']['stmt2']['statement_name_bin']


@pytest.mark.parametrize("test_case", [
    {
        "name": "max_prepared_statements_zero",
        "max_prepared_statements": 0,
        "queries": ["SELECT 1", "SELECT 2"],
        "expected_close_calls": 0
    },
    {
        "name": "max_prepared_statements_default",
        "max_prepared_statements": 1000,
        "queries": ["SELECT 1", "SELECT 2"],
        "expected_close_calls": 0
    },
    {
        "name": "max_prepared_statements_limit_1",
        "max_prepared_statements": 2,
        "queries": ["SELECT 1", "SELECT 2", "SELECT 3"],
        "expected_close_calls": 2
    },
{
        "name": "max_prepared_statements_limit_2",
        "max_prepared_statements": 2,
        "queries": ["SELECT 1", "SELECT 2"],
        "expected_close_calls": 0
    }
])
def test_max_prepared_statement_zero(mocker, test_case):
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


@pytest.mark.parametrize("test_case", [
    {
        "max_prepared_statements" : 0,
        "statement_name" : "stmt1",
        "expected_statement_name" : ""
    },
    {
        "max_prepared_statements" : 1000,
        "statement_name" : "stmt1",
        "expected_statement_name" : "stmt1"
    }
])
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
    assert mock_connection.get_statement_name_bin(test_case["statement_name"]) == test_case["expected_statement_name"].encode("ascii") + b"\x00"

@pytest.mark.parametrize("max_prepared_statements,expected_result", [
    (0, 0),
    (10, 10),
    (-1, 1000) # Invalid input results in default value
])
def test_get_max_prepared_statement(max_prepared_statements, expected_result):
    """
    Test the get_max_prepared_statement method of the Connection class.

    This test verifies that the method correctly returns the max_prepared_statements
    value even when the user inputs an invalid number.
    """
    # Setup Connection instance
    mock_connection = Connection.__new__(Connection)
    assert mock_connection.get_max_prepared_statement(max_prepared_statements) == expected_result