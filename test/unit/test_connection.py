import socket
import typing
from collections import deque
from decimal import Decimal
from test.utils import pandas_only
from unittest import mock
from unittest.mock import patch

import pytest  # type: ignore

from redshift_connector import (
    Connection,
    Cursor,
    Error,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    validate_keepalive_values,
)
from redshift_connector.config import (
    ClientProtocolVersion,
    max_int2,
    max_int4,
    max_int8,
    min_int2,
    min_int4,
    min_int8,
)
from redshift_connector.utils.type_utils import py_types as PY_TYPES
from redshift_connector.utils.type_utils import redshift_types as REDSHIFT_TYPES

test_error_responses_data: typing.List[typing.Tuple[bytes, typing.Dict, typing.Type[Error]]] = [
    (
        (
            b"SERROR\x00"
            b"C42704\x00"
            b'Mtype "trash" does not exist\x00F/'
            b"home/ec2-user/padb/src/pg/src/backend/parser/parse_type.c\x00"
            b"L211\x00"
            b"RtypenameType\x00\x00"
        ),
        {
            "S": "ERROR",
            "C": "42704",
            "M": 'type "trash" does not exist',
            "F": "/home/ec2-user/padb/src/pg/src/backend/parser/parse_type.c",
            "L": "211",
            "R": "typenameType",
        },
        ProgrammingError,
    ),
    (
        (
            b"SERROR\x00"
            b"C28000\x00"
            b"MHello world\x00"
            b"F/home/ec2-user/padb/src/pg/src/backend/path/to/something.c\x00"
            b"L111\x00"
            b"RThisShouldBeAnInterfaceError\x00\x00"
        ),
        {
            "S": "ERROR",
            "C": "28000",
            "M": "Hello world",
            "F": "/home/ec2-user/padb/src/pg/src/backend/path/to/something.c",
            "L": "111",
            "R": "ThisShouldBeAnInterfaceError",
        },
        InterfaceError,
    ),
    (
        (
            b"SERROR\x00"
            b"C23505\x00"
            b"MHello world\x00"
            b"F/home/ec2-user/padb/src/pg/src/backend/path/to/something.c\x00"
            b"L111\x00"
            b"RThisShouldBeAnIntegrityError\x00\x00"
        ),
        {
            "S": "ERROR",
            "C": "23505",
            "M": "Hello world",
            "F": "/home/ec2-user/padb/src/pg/src/backend/path/to/something.c",
            "L": "111",
            "R": "ThisShouldBeAnIntegrityError",
        },
        IntegrityError,
    ),
    (
        (
            b"SERROR\x00"
            b"C42601\x00"
            b'Msyntax error at or near "hello"\x00'
            b"P19\x00"
            b"F/home/ec2-user/padb/src/pg/src/backend/parser/parser_scan.l\x00"
            b"L718\x00"
            b"Ryyerror\x00\x00"
        ),
        {
            "S": "ERROR",
            "C": "42601",
            "M": 'syntax error at or near "hello"',
            "P": "19",
            "F": "/home/ec2-user/padb/src/pg/src/backend/parser/parser_scan.l",
            "L": "718",
            "R": "yyerror",
        },
        ProgrammingError,
    ),
    (
        (
            b"SERROR\x00"
            b"C42601\x00"
            b"Msyntax error at or near \"b'redshift_connector_sn\\xef\\xbd\\xafw'\"\x00"
            b"P26\x00"
            b"F/home/ec2-user/padb/src/pg/src/backend/parser/parser_scan.l\x00"
            b"L718\x00"
            b"Ryyerror\x00\x00"
        ),
        {
            "S": "ERROR",
            "C": "42601",
            "M": "syntax error at or near \"b'redshift_connector_sn\\xef\\xbd\\xafw'\"",
            "P": "26",
            "F": "/home/ec2-user/padb/src/pg/src/backend/parser/parser_scan.l",
            "L": "718",
            "R": "yyerror",
        },
        ProgrammingError,
    ),
]


@pytest.mark.parametrize("_input", test_error_responses_data)
def test_handle_error_response(_input) -> None:
    server_msg, expected_decoded_msg, expected_error = _input
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.handle_ERROR_RESPONSE(server_msg, None)
    assert type(mock_connection.error) == expected_error
    assert str(expected_decoded_msg) in str(mock_connection.error)


test_inspect_int_vals: typing.List[typing.Tuple[int, typing.Tuple[int, int, typing.Callable]]] = [
    (min_int2 - 1, PY_TYPES[23]),
    (min_int2, PY_TYPES[23]),
    (min_int2 + 1, PY_TYPES[21]),
    (max_int2 - 1, PY_TYPES[21]),
    (max_int2, PY_TYPES[23]),
    (max_int2 + 1, PY_TYPES[23]),
    (min_int4 - 1, PY_TYPES[20]),
    (min_int4, PY_TYPES[20]),
    (min_int4 + 1, PY_TYPES[23]),
    (max_int4 - 1, PY_TYPES[23]),
    (max_int4, PY_TYPES[20]),
    (max_int4 + 1, PY_TYPES[20]),
    (min_int8 - 1, PY_TYPES[Decimal]),
    (min_int8, PY_TYPES[Decimal]),
    (min_int8 + 1, PY_TYPES[20]),
    (max_int8 - 1, PY_TYPES[20]),
    (max_int8, PY_TYPES[Decimal]),
    (max_int8 + 1, PY_TYPES[Decimal]),
]


@pytest.mark.parametrize("_input", test_inspect_int_vals)
def test_inspect_int(_input) -> None:
    input_val, expected_type = _input
    mock_connection = Connection.__new__(Connection)
    mock_connection.py_types = PY_TYPES
    assert mock_connection.inspect_int(input_val) == expected_type


test_row_description_extended_metadata = [
    (
        b"\x00\x01proname\x00\x00\x00\x04\xe7\x00\x01\x00\x00\x00\x13\x00\x80\xff\xff\xff\xff\x00\x00pg_catalog\x00pg_proc\x00proname\x00dev\x00\x10\x01",
        [
            {
                "table_oid": 1255,
                "column_attrnum": 1,
                "type_oid": 19,
                "type_size": 128,
                "type_modifier": -1,
                "format": 0,
                "label": b"proname",
                "schema_name": b"pg_catalog",
                "table_name": b"pg_proc",
                "column_name": b"proname",
                "catalog_name": b"dev",
                "nullable": 1,
                "autoincrement": 0,
                "read_only": 0,
                "searchable": 1,
                "redshift_connector_fc": 1,
            }
        ],
    ),
]


@pytest.mark.parametrize("_input", test_row_description_extended_metadata)
@pytest.mark.parametrize("protocol", [ClientProtocolVersion.EXTENDED_RESULT_METADATA])
def test_handle_row_description_extended_metadata(_input, protocol) -> None:
    data, exp_result = _input
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection._client_protocol_version = protocol
    mock_connection.redshift_types = dict(REDSHIFT_TYPES)  # type: ignore
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = {"row_desc": []}

    mock_connection.handle_ROW_DESCRIPTION(data, mock_cursor)
    assert mock_cursor.ps is not None
    assert "row_desc" in mock_cursor.ps
    assert len(mock_cursor.ps["row_desc"]) == 1
    assert exp_result[0].items() <= mock_cursor.ps["row_desc"][0].items()
    assert "func" in mock_cursor.ps["row_desc"][0]


test_row_description_base: typing.List[typing.Tuple[bytes, typing.List[typing.Dict[str, typing.Union[int, bytes]]]]] = [
    (
        b"\x00\x01proname\x00\x00\x00\x04\xe7\x00\x01\x00\x00\x00\x13\x00\x80\xff\xff\xff\xff\x00\x00pg_catalog\x00pg_proc\x00proname\x00dev\x00\x10\x01",
        [
            {
                "table_oid": 1255,
                "column_attrnum": 1,
                "type_oid": 19,
                "type_size": 128,
                "type_modifier": -1,
                "format": 0,
                "label": b"proname",
                "redshift_connector_fc": 1,
            }
        ],
    )
]


@pytest.mark.parametrize("_input", test_row_description_base)
def test_handle_row_description_base(_input) -> None:
    data, exp_result = _input
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection._client_protocol_version = ClientProtocolVersion.BASE_SERVER.value
    mock_connection.redshift_types = dict(REDSHIFT_TYPES)  # type: ignore
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = {"row_desc": []}

    mock_connection.handle_ROW_DESCRIPTION(data, mock_cursor)
    assert mock_cursor.ps is not None
    assert "row_desc" in mock_cursor.ps
    assert len(mock_cursor.ps["row_desc"]) == 1
    assert exp_result[0].items() <= mock_cursor.ps["row_desc"][0].items()
    assert "func" in mock_cursor.ps["row_desc"][0]


def test_handle_row_description_missing_ps_raises() -> None:
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None

    with pytest.raises(InterfaceError, match="Cursor is missing prepared statement"):
        mock_connection.handle_ROW_DESCRIPTION(b"\x00", mock_cursor)


def test_handle_row_description_missing_row_desc_raises() -> None:
    mock_connection: Connection = Connection.__new__(Connection)
    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = {}

    with pytest.raises(InterfaceError, match="Prepared Statement is missing row description"):
        mock_connection.handle_ROW_DESCRIPTION(b"\x00", mock_cursor)


test_is_multidatabases_catalog_enable_in_server_data: typing.List[typing.Tuple[typing.Optional[str], bool]] = [
    ("on", True),
    ("off", False),
    ("garbage", False),
    (None, False),
]


@pytest.mark.parametrize("_input", test_is_multidatabases_catalog_enable_in_server_data)
def test_is_multidatabases_catalog_enable_in_server(_input) -> None:
    param_status, exp_val = _input
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque()

    if param_status is not None:
        mock_connection.parameter_statuses.append((b"datashare_enabled", param_status.encode()))

    assert mock_connection._is_multi_databases_catalog_enable_in_server == exp_val


test_is_cross_datasharing_enable_in_server_data: typing.List[typing.Tuple[typing.Optional[str], bool]] = [
    ("on", True),
    ("off", False),
    (None, False),
]


@pytest.mark.parametrize("_input", test_is_cross_datasharing_enable_in_server_data)
def test_is_cross_datasharing_enable_in_server(_input):
    param_status, exp_val = _input
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses: deque = deque()

    if param_status is not None:
        mock_connection.parameter_statuses.append((b"external_database", param_status.encode()))

    assert mock_connection._is_cross_datasharing_enable_in_server == exp_val


def test_is_cross_datasharing_enable_in_server_raises_on_invalid():
    param_status = "garbage"
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses: deque = deque()

    if param_status is not None:
        mock_connection.parameter_statuses.append((b"external_database", param_status.encode()))

    with pytest.raises(
        InterfaceError,
        match="Protocol error. Session setup failed. Invalid value of external_database parameter. Only on/off are valid values",
    ):
        mock_connection._is_cross_datasharing_enable_in_server


test_is_single_database_metadata_data: typing.List[
    typing.Tuple[typing.Optional[str], typing.Optional[str], bool, bool]
] = [
    ("on", "on", True, False),
    ("on", "on", False, False),
    ("on", None, True, True),
    ("on", None, False, False),
    (None, "on", True, False),
    (None, "on", False, False),
    ("off", "on", True, False),
    ("off", "on", False, False),
    ("on", "off", True, True),
    ("on", "off", False, False),
    ("off", "off", False, True),
    ("off", "off", True, True),
    ("off", None, False, True),
    ("off", None, True, True),
    (None, "off", False, True),
    (None, "off", True, True),
    (None, None, True, True),
    (None, None, False, True),
]


@pytest.mark.parametrize("_input", test_is_single_database_metadata_data)
def test_is_single_database_metadata(_input):
    datashare_enabled, dsw_enabled, database_metadata_current_db_only_val, exp_val = _input

    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection.parameter_statuses = deque()
    mock_connection._database_metadata_current_db_only = database_metadata_current_db_only_val

    if datashare_enabled is not None:
        mock_connection.parameter_statuses.append((b"datashare_enabled", datashare_enabled.encode()))
    if dsw_enabled is not None:
        mock_connection.parameter_statuses.append((b"external_database", dsw_enabled.encode()))

    assert mock_connection.is_single_database_metadata == exp_val


def test_client_os_version_is_present() -> None:
    mock_connection: Connection = Connection.__new__(Connection)
    assert mock_connection.client_os_version is not None
    assert isinstance(mock_connection.client_os_version, str)


def test_client_os_version_is_not_present() -> None:
    mock_connection: Connection = Connection.__new__(Connection)

    with patch("platform.platform", side_effect=Exception("not for you")):
        assert mock_connection.client_os_version == "unknown"


def test_socket_timeout_error() -> None:
    with mock.patch("socket.socket.connect") as mock_socket:
        mock_socket.side_effect = socket.timeout
        with pytest.raises(OperationalError):
            Connection(user="mock_user", password="mock_password", host="localhost", port=8080, database="mocked")


def mock_read(*args, **kwargs):
    return b""


def test_handle_messages_broken_pipe_blocking() -> None:
    # mock the connection and mock the read attribute
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection._read = mock_read

    # we only need to mock the raw socket
    mock_usock = mock.Mock()
    mock_usock.timeout = None
    mock_connection._usock = mock_usock

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None

    with pytest.raises(
        InterfaceError,
        match="BrokenPipe: server socket closed. Please check that client side networking configurations such "
        "as Proxies, firewalls, VPN, etc. are not affecting your network connection.",
    ):
        mock_connection.handle_messages(mock_cursor)


def test_handle_messages_broken_pipe_timeout() -> None:
    # mock the connection and mock the read attribute
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection._read = mock_read

    # we only need to mock the raw socket
    mock_usock = mock.Mock()
    mock_usock.timeout = 47
    mock_connection._usock = mock_usock

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None

    with pytest.raises(
        InterfaceError,
        match="BrokenPipe: server socket closed. We noticed a timeout is set for this connection. Consider "
        "raising the timeout or defaulting timeout to none.",
    ):
        mock_connection.handle_messages(mock_cursor)


def test_handle_messages_merge_socket_read_broken_pipe_blocking() -> None:
    # mock the connection and mock the read attribute
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection._read = mock_read

    # we only need to mock the raw socket
    mock_usock = mock.Mock()
    mock_usock.timeout = None
    mock_connection._usock = mock_usock

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None

    with pytest.raises(
        InterfaceError,
        match="BrokenPipe: server socket closed. Please check that client side networking configurations such "
        "as Proxies, firewalls, VPN, etc. are not affecting your network connection.",
    ):
        mock_connection.handle_messages_merge_socket_read(mock_cursor)


def test_handle_messages_merge_socket_read_broken_pipe_timeout() -> None:
    # mock the connection and mock the read attribute
    mock_connection: Connection = Connection.__new__(Connection)
    mock_connection._read = mock_read

    # we only need to mock the raw socket
    mock_usock = mock.Mock()
    mock_usock.timeout = 47
    mock_connection._usock = mock_usock

    mock_cursor: Cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None

    with pytest.raises(
        InterfaceError,
        match="BrokenPipe: server socket closed. We noticed a timeout is set for this connection. Consider "
        "raising the timeout or defaulting timeout to none.",
    ):
        mock_connection.handle_messages_merge_socket_read(mock_cursor)


def test_broken_pipe_on_connect(db_kwargs) -> None:
    db_kwargs["ssl"] = False

    with mock.patch("socket.getaddrinfo") as mock_getaddrinfo:
        addr_tuple = [(0, 1, 2, "", ("3.226.18.73", 5439)), (2, 1, 6, "", ("3.226.18.73", 5439))]
        mock_getaddrinfo.return_value = addr_tuple
        with mock.patch("socket.socket.connect") as mock_usock:
            mock_usock.side_effect = lambda *args, **kwargs: None
            with mock.patch("socket.socket.makefile") as mock_sock:
                mock_file = mock_sock.return_value
                mock_file._read.return_value = b""
                with pytest.raises(
                    InterfaceError,
                    match="BrokenPipe: server socket closed. Please check that client side networking configurations such "
                    "as Proxies, firewalls, VPN, etc. are not affecting your network connection.",
                ):
                    db_kwargs.pop("region")
                    db_kwargs.pop("cluster_identifier")
                    Connection(**db_kwargs)


def test_broken_pipe_timeout_on_connect(db_kwargs) -> None:
    db_kwargs["ssl"] = False
    db_kwargs["timeout"] = 60

    with mock.patch("socket.getaddrinfo") as mock_getaddrinfo:
        addr_tuple = [(0, 1, 2, "", ("3.226.18.73", 5439)), (2, 1, 6, "", ("3.226.18.73", 5439))]
        mock_getaddrinfo.return_value = addr_tuple
        with mock.patch("socket.socket.connect") as mock_usock:
            mock_usock.side_effect = lambda *args, **kwargs: None

            with mock.patch("socket.socket.makefile") as mock_sock:
                mock_file = mock_sock.return_value
                mock_file._read.return_value = b""
                with pytest.raises(
                    InterfaceError,
                    match="BrokenPipe: server socket closed. We noticed a timeout is set for this connection. Consider "
                    "raising the timeout or defaulting timeout to none.",
                ):
                    db_kwargs.pop("region")
                    db_kwargs.pop("cluster_identifier")
                    Connection(**db_kwargs)


def make_mock_connection(db_kwargs):
    db_kwargs["ssl"] = False
    db_kwargs["timeout"] = 60

    with mock.patch("socket.getaddrinfo") as mock_getaddrinfo:
        addr_tuple = [(0, 1, 2, "", ("3.226.18.73", 5439)), (2, 1, 6, "", ("3.226.18.73", 5439))]
        mock_getaddrinfo.return_value = addr_tuple
        with mock.patch("socket.socket.connect") as mock_usock:
            mock_usock.side_effect = lambda *args, **kwargs: None

            with mock.patch("socket.socket.makefile") as mock_sock:
                mock_file = mock_sock.return_value
                mock_file.read.return_value = b"Zasej"
                db_kwargs.pop("region")
                db_kwargs.pop("cluster_identifier")
                return Connection(**db_kwargs)


@pandas_only
def test_make_params_maps_pandas_timestamp_to_timestamp(db_kwargs):
    import datetime

    import pandas as pd

    from redshift_connector.utils.oids import RedshiftOID
    from redshift_connector.utils.type_utils import py_types, timestamptz_send_integer

    columns = ["dw_inserted_at"]
    values = pd.DataFrame({col: [datetime.datetime.now(datetime.timezone.utc)] * 1 for col in columns}).values.tolist()[
        0
    ]

    mock_connection: Connection = make_mock_connection(db_kwargs)
    res = mock_connection.make_params(values)
    assert res[0][0] == RedshiftOID.TIMESTAMPTZ
    assert res[0][1] == 1
    assert res[0][2] == timestamptz_send_integer


@pytest.mark.parametrize(
    "idle,interval,count,expected_error",
    [
        (-1, None, None, "tcp_keepalive_idle must be positive"),
        (None, -1, None, "tcp_keepalive_interval must be positive"),
        (None, None, -1, "tcp_keepalive_count must be positive"),
        (0, None, None, "tcp_keepalive_idle must be positive"),
        (None, 0, None, "tcp_keepalive_interval must be positive"),
        (None, None, 0, "tcp_keepalive_count must be positive"),
        (1, 1, 1, None),  # valid values should not raise error
    ],
)
def test_tcp_keepalive_validation(idle, interval, count, expected_error):
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            validate_keepalive_values(idle, interval, count)
    else:
        # Should not raise any exception
        validate_keepalive_values(idle, interval, count)
