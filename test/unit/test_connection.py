import typing
from decimal import Decimal

import pytest  # type: ignore

from redshift_connector import (
    Connection,
    Cursor,
    Error,
    IntegrityError,
    InterfaceError,
    ProgrammingError,
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
from redshift_connector.utils.type_utils import py_types

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
def test_handle_ERROR_RESPONSE(_input):
    server_msg, expected_decoded_msg, expected_error = _input
    mock_connection = Connection.__new__(Connection)
    mock_connection.handle_ERROR_RESPONSE(server_msg, None)
    assert type(mock_connection.error) == expected_error
    assert str(expected_decoded_msg) in str(mock_connection.error)


def test_handle_COPY_DONE():
    mock_connection = Connection.__new__(Connection)
    assert hasattr(mock_connection, "_copy_done") is False
    mock_connection.handle_COPY_DONE(None, None)
    assert mock_connection._copy_done is True


test_inspect_int_vals: typing.List[typing.Tuple[int, typing.Tuple[int, int, typing.Callable]]] = [
    (min_int2 - 1, py_types[23]),
    (min_int2, py_types[23]),
    (min_int2 + 1, py_types[21]),
    (max_int2 - 1, py_types[21]),
    (max_int2, py_types[23]),
    (max_int2 + 1, py_types[23]),
    (min_int4 - 1, py_types[20]),
    (min_int4, py_types[20]),
    (min_int4 + 1, py_types[23]),
    (max_int4 - 1, py_types[23]),
    (max_int4, py_types[20]),
    (max_int4 + 1, py_types[20]),
    (min_int8 - 1, py_types[Decimal]),
    (min_int8, py_types[Decimal]),
    (min_int8 + 1, py_types[20]),
    (max_int8 - 1, py_types[20]),
    (max_int8, py_types[Decimal]),
    (max_int8 + 1, py_types[Decimal]),
]


@pytest.mark.parametrize("_input", test_inspect_int_vals)
def test_inspect_int(_input):
    input_val, expected_type = _input
    mock_connection = Connection.__new__(Connection)
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
                "pg8000_fc": 1,
            }
        ],
    ),
]


@pytest.mark.parametrize("_input", test_row_description_extended_metadata)
@pytest.mark.parametrize("protocol", [ClientProtocolVersion.EXTENDED_RESULT_METADATA])
def test_handle_ROW_DESCRIPTION_extended_metadata(_input, protocol):
    data, exp_result = _input
    mock_connection = Connection.__new__(Connection)
    mock_connection._client_protocol_version = protocol
    mock_cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = {"row_desc": []}

    mock_connection.handle_ROW_DESCRIPTION(data, mock_cursor)
    assert mock_cursor.ps is not None
    assert "row_desc" in mock_cursor.ps
    assert len(mock_cursor.ps["row_desc"]) == 1
    assert exp_result[0].items() <= mock_cursor.ps["row_desc"][0].items()
    assert "func" in mock_cursor.ps["row_desc"][0]


test_row_description_base = [
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
                "pg8000_fc": 1,
            }
        ],
    )
]


@pytest.mark.parametrize("_input", test_row_description_base)
def test_handle_ROW_DESCRIPTION_base(_input):
    data, exp_result = _input
    mock_connection = Connection.__new__(Connection)
    mock_connection._client_protocol_version = ClientProtocolVersion.BASE_SERVER.value
    mock_cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = {"row_desc": []}

    mock_connection.handle_ROW_DESCRIPTION(data, mock_cursor)
    assert mock_cursor.ps is not None
    assert "row_desc" in mock_cursor.ps
    assert len(mock_cursor.ps["row_desc"]) == 1
    assert exp_result[0].items() <= mock_cursor.ps["row_desc"][0].items()
    assert "func" in mock_cursor.ps["row_desc"][0]


def test_handle_ROW_DESCRIPTION_missing_ps_raises():
    mock_connection = Connection.__new__(Connection)
    mock_cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = None

    with pytest.raises(InterfaceError, match="Cursor is missing prepared statement"):
        mock_connection.handle_ROW_DESCRIPTION(b"\x00", mock_cursor)


def test_handle_ROW_DESCRIPTION_missing_row_desc_raises():
    mock_connection = Connection.__new__(Connection)
    mock_cursor = Cursor.__new__(Cursor)
    mock_cursor.ps = {}

    with pytest.raises(InterfaceError, match="Prepared Statement is missing row description"):
        mock_connection.handle_ROW_DESCRIPTION(b"\x00", mock_cursor)
