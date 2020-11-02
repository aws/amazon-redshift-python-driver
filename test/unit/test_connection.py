import typing
from decimal import Decimal

import pytest  # type: ignore

from redshift_connector import (
    Connection,
    Error,
    IntegrityError,
    InterfaceError,
    ProgrammingError,
)
from redshift_connector.config import (
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
