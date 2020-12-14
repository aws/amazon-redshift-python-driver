import os
import socket
import typing
from collections import deque
from copy import deepcopy
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from decimal import Decimal
from distutils.version import LooseVersion
from hashlib import md5
from itertools import count
from os import getpid
from struct import pack
from typing import TYPE_CHECKING
from warnings import warn

from scramp import ScramClient  # type: ignore

from redshift_connector.config import (
    DEFAULT_PROTOCOL_VERSION,
    ClientProtocolVersion,
    _client_encoding,
    max_int2,
    max_int4,
    max_int8,
    min_int2,
    min_int4,
    min_int8,
    pg_array_types,
    pg_to_py_encodings,
)
from redshift_connector.cursor import Cursor
from redshift_connector.error import (
    ArrayContentNotHomogenousError,
    ArrayContentNotSupportedError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from redshift_connector.utils import (
    FC_BINARY,
    NULL,
    NULL_BYTE,
    array_check_dimensions,
    array_dim_lengths,
    array_find_first_element,
    array_flatten,
    array_has_null,
    bh_unpack,
    cccc_unpack,
    ci_unpack,
    h_pack,
    h_unpack,
    i_pack,
    i_unpack,
    ihihih_unpack,
    ii_pack,
    iii_pack,
    pg_types,
    py_types,
    q_pack,
    walk_array,
)

if TYPE_CHECKING:
    from ssl import SSLSocket

# Copyright (c) 2007-2009, Mathieu Fenniak
# Copyright (c) The Contributors
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"

ZERO: Timedelta = Timedelta(0)
BINARY: type = bytes


# The purpose of this function is to change the placeholder of original query into $1, $2
# in order to be identified by database
# example: INSERT INTO book (title) VALUES (:title) -> INSERT INTO book (title) VALUES ($1)
# also return the function: make_args()
def convert_paramstyle(style: str, query) -> typing.Tuple[str, typing.Any]:
    # I don't see any way to avoid scanning the query string char by char,
    # so we might as well take that careful approach and create a
    # state-based scanner.  We'll use int variables for the state.
    OUTSIDE: int = 0  # outside quoted string
    INSIDE_SQ: int = 1  # inside single-quote string '...'
    INSIDE_QI: int = 2  # inside quoted identifier   "..."
    INSIDE_ES: int = 3  # inside escaped single-quote string, E'...'
    INSIDE_PN: int = 4  # inside parameter name eg. :name
    INSIDE_CO: int = 5  # inside inline comment eg. --

    in_quote_escape: bool = False
    in_param_escape: bool = False
    placeholders: typing.List[str] = []
    output_query: typing.List[str] = []
    param_idx: typing.Iterator[str] = map(lambda x: "$" + str(x), count(1))
    state: int = OUTSIDE
    prev_c: typing.Optional[str] = None
    for i, c in enumerate(query):
        if i + 1 < len(query):
            next_c = query[i + 1]
        else:
            next_c = None

        if state == OUTSIDE:
            if c == "'":
                output_query.append(c)
                if prev_c == "E":
                    state = INSIDE_ES
                else:
                    state = INSIDE_SQ
            elif c == '"':
                output_query.append(c)
                state = INSIDE_QI
            elif c == "-":
                output_query.append(c)
                if prev_c == "-":
                    state = INSIDE_CO
            elif style == "qmark" and c == "?":
                output_query.append(next(param_idx))
            elif style == "numeric" and c == ":" and next_c not in ":=" and prev_c != ":":
                # Treat : as beginning of parameter name if and only
                # if it's the only : around
                # Needed to properly process type conversions
                # i.e. sum(x)::float
                output_query.append("$")
            elif style == "named" and c == ":" and next_c not in ":=" and prev_c != ":":
                # Same logic for : as in numeric parameters
                state = INSIDE_PN
                placeholders.append("")
            elif style == "pyformat" and c == "%" and next_c == "(":
                state = INSIDE_PN
                placeholders.append("")
            elif style in ("format", "pyformat") and c == "%":
                style = "format"
                if in_param_escape:
                    in_param_escape = False
                    output_query.append(c)
                else:
                    if next_c == "%":
                        in_param_escape = True
                    elif next_c == "s":
                        state = INSIDE_PN
                        output_query.append(next(param_idx))
                    else:
                        raise InterfaceError("Only %s and %% are supported in the query.")
            else:
                output_query.append(c)

        elif state == INSIDE_SQ:
            if c == "'":
                if in_quote_escape:
                    in_quote_escape = False
                else:
                    if next_c == "'":
                        in_quote_escape = True
                    else:
                        state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_QI:
            if c == '"':
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_ES:
            if c == "'" and prev_c != "\\":
                # check for escaped single-quote
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_PN:
            if style == "named":
                placeholders[-1] += c
                if next_c is None or (not next_c.isalnum() and next_c != "_"):
                    state = OUTSIDE
                    try:
                        pidx: int = placeholders.index(placeholders[-1], 0, -1)
                        output_query.append("$" + str(pidx + 1))
                        del placeholders[-1]
                    except ValueError:
                        output_query.append("$" + str(len(placeholders)))
            elif style == "pyformat":
                if prev_c == ")" and c == "s":
                    state = OUTSIDE
                    try:
                        pidx = placeholders.index(placeholders[-1], 0, -1)
                        output_query.append("$" + str(pidx + 1))
                        del placeholders[-1]
                    except ValueError:
                        output_query.append("$" + str(len(placeholders)))
                elif c in "()":
                    pass
                else:
                    placeholders[-1] += c
            elif style == "format":
                state = OUTSIDE

        elif state == INSIDE_CO:
            output_query.append(c)
            if c == "\n":
                state = OUTSIDE

        prev_c = c

    if style in ("numeric", "qmark", "format"):

        def make_args(vals):
            return vals

    else:

        def make_args(vals):
            return tuple(vals[p] for p in placeholders)

    return "".join(output_query), make_args


# Message codes
# ALl communication is through a stream of messages
# Driver will send one or more messages to database,
# and database will respond one or more messages
# The first byte of a message specify the type of the message
NOTICE_RESPONSE: bytes = b"N"
AUTHENTICATION_REQUEST: bytes = b"R"
PARAMETER_STATUS: bytes = b"S"
BACKEND_KEY_DATA: bytes = b"K"
READY_FOR_QUERY: bytes = b"Z"
ROW_DESCRIPTION: bytes = b"T"
ERROR_RESPONSE: bytes = b"E"
DATA_ROW: bytes = b"D"
COMMAND_COMPLETE: bytes = b"C"
PARSE_COMPLETE: bytes = b"1"
BIND_COMPLETE: bytes = b"2"
CLOSE_COMPLETE: bytes = b"3"
PORTAL_SUSPENDED: bytes = b"s"
NO_DATA: bytes = b"n"
PARAMETER_DESCRIPTION: bytes = b"t"
NOTIFICATION_RESPONSE: bytes = b"A"
COPY_DONE: bytes = b"c"
COPY_DATA: bytes = b"d"
COPY_IN_RESPONSE: bytes = b"G"
COPY_OUT_RESPONSE: bytes = b"H"
EMPTY_QUERY_RESPONSE: bytes = b"I"

BIND: bytes = b"B"
PARSE: bytes = b"P"
EXECUTE: bytes = b"E"
FLUSH: bytes = b"H"
SYNC: bytes = b"S"
PASSWORD: bytes = b"p"
DESCRIBE: bytes = b"D"
TERMINATE: bytes = b"X"
CLOSE: bytes = b"C"


# This inform the format of a message
# the first byte, the code, will be the type of the message
# then add the 4 bytes to inform the length of rest of message
# then add the real data we want to send
def create_message(code: bytes, data: bytes = b"") -> bytes:
    return code + typing.cast(bytes, i_pack(len(data) + 4)) + data


FLUSH_MSG: bytes = create_message(FLUSH)
SYNC_MSG: bytes = create_message(SYNC)
TERMINATE_MSG: bytes = create_message(TERMINATE)
COPY_DONE_MSG: bytes = create_message(COPY_DONE)
EXECUTE_MSG: bytes = create_message(EXECUTE, NULL_BYTE + i_pack(0))

# DESCRIBE constants
STATEMENT: bytes = b"S"
PORTAL: bytes = b"P"

# ErrorResponse codes
RESPONSE_SEVERITY: str = "S"  # always present
RESPONSE_SEVERITY = "V"  # always present
RESPONSE_CODE: str = "C"  # always present
RESPONSE_MSG: str = "M"  # always present
RESPONSE_DETAIL: str = "D"
RESPONSE_HINT: str = "H"
RESPONSE_POSITION: str = "P"
RESPONSE__POSITION: str = "p"
RESPONSE__QUERY: str = "q"
RESPONSE_WHERE: str = "W"
RESPONSE_FILE: str = "F"
RESPONSE_LINE: str = "L"
RESPONSE_ROUTINE: str = "R"

IDLE: bytes = b"I"
IDLE_IN_TRANSACTION: bytes = b"T"
IDLE_IN_FAILED_TRANSACTION: bytes = b"E"

arr_trans: typing.Mapping[int, typing.Optional[str]] = dict(zip(map(ord, "[] 'u"), ["{", "}", None, None, None]))


class Connection:
    # DBAPI Extension: supply exceptions as attributes on the connection
    Warning = property(lambda self: self._getError(Warning))
    Error = property(lambda self: self._getError(Error))
    InterfaceError = property(lambda self: self._getError(InterfaceError))
    DatabaseError = property(lambda self: self._getError(DatabaseError))
    OperationalError = property(lambda self: self._getError(OperationalError))
    IntegrityError = property(lambda self: self._getError(IntegrityError))
    InternalError = property(lambda self: self._getError(InternalError))
    ProgrammingError = property(lambda self: self._getError(ProgrammingError))
    NotSupportedError = property(lambda self: self._getError(NotSupportedError))

    def __enter__(self: "Connection") -> "Connection":
        return self

    def __exit__(self: "Connection", exc_type, exc_value, traceback) -> None:
        self.close()

    def _getError(self: "Connection", error):
        warn("DB-API extension connection.%s used" % error.__name__, stacklevel=3)
        return error

    def __init__(
        self: "Connection",
        user: str,
        password: str,
        database: str,
        host: str = "localhost",
        port: int = 5439,
        source_address: typing.Optional[str] = None,
        unix_sock: typing.Optional[str] = None,
        ssl: bool = True,
        sslmode: str = "verify-ca",
        timeout: typing.Optional[int] = None,
        max_prepared_statements: int = 1000,
        tcp_keepalive: typing.Optional[bool] = True,
        application_name: typing.Optional[str] = None,
        replication: typing.Optional[str] = None,
        client_protocol_version: int = DEFAULT_PROTOCOL_VERSION,
        database_metadata_current_db_only: bool = True,
    ):

        self.merge_socket_read = False

        _client_encoding = "utf8"
        self._commands_with_count: typing.Tuple[bytes, ...] = (
            b"INSERT",
            b"DELETE",
            b"UPDATE",
            b"MOVE",
            b"FETCH",
            b"COPY",
            b"SELECT",
        )
        self.notifications: deque = deque(maxlen=100)
        self.notices: deque = deque(maxlen=100)
        self.parameter_statuses: deque = deque(maxlen=100)
        self.max_prepared_statements: int = int(max_prepared_statements)
        self._run_cursor: Cursor = Cursor(self, paramstyle="named")
        self._client_protocol_version: int = client_protocol_version
        self._database = database
        self._database_metadata_current_db_only: bool = database_metadata_current_db_only

        if user is None:
            raise InterfaceError("The 'user' connection parameter cannot be None")

        init_params: typing.Dict[str, typing.Optional[typing.Union[str, bytes]]] = {
            "user": user,
            "database": database,
            "application_name": application_name,
            "replication": replication,
            "client_protocol_version": str(self._client_protocol_version),
        }

        for k, v in tuple(init_params.items()):
            if isinstance(v, str):
                init_params[k] = v.encode("utf8")
            elif v is None:
                del init_params[k]
            elif not isinstance(v, (bytes, bytearray)):
                raise InterfaceError("The parameter " + k + " can't be of type " + str(type(v)) + ".")

        self.user: bytes = typing.cast(bytes, init_params["user"])

        if isinstance(password, str):
            self.password: bytes = password.encode("utf8")
        else:
            self.password = password

        self.autocommit: bool = False
        self._xid = None

        self._caches: typing.Dict = {}

        # Create the TCP/Ip socket and connect to specific database
        # if there already has a socket, it will not create new connection when run connect again
        try:
            if unix_sock is None and host is not None:
                self._usock: typing.Union[socket.socket, "SSLSocket"] = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM
                )
                if source_address is not None:
                    self._usock.bind((source_address, 0))
            elif unix_sock is not None:
                if not hasattr(socket, "AF_UNIX"):
                    raise InterfaceError("attempt to connect to unix socket on unsupported " "platform")
                self._usock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            else:
                raise ProgrammingError("one of host or unix_sock must be provided")
            if timeout is not None:
                self._usock.settimeout(timeout)

            if unix_sock is None and host is not None:
                self._usock.connect((host, port))
            elif unix_sock is not None:
                self._usock.connect(unix_sock)

            # For Redshift, we the default ssl approve is True
            # create ssl connection with Redshift CA certificates and check the hostname
            if ssl is True:
                try:
                    from ssl import CERT_REQUIRED, SSLContext

                    # ssl_context = ssl.create_default_context()

                    path = os.path.abspath(__file__)
                    path = "/".join(path.split("/")[:-1]) + "/files/redshift-ca-bundle.crt"

                    ssl_context: SSLContext = SSLContext()
                    ssl_context.verify_mode = CERT_REQUIRED
                    ssl_context.load_verify_locations(path)

                    # Int32(8) - Message length, including self.
                    # Int32(80877103) - The SSL request code.
                    self._usock.sendall(ii_pack(8, 80877103))
                    resp: bytes = self._usock.recv(1)
                    if resp != b"S":
                        raise InterfaceError("Server refuses SSL")

                    if sslmode == "verify-ca":
                        self._usock = ssl_context.wrap_socket(self._usock)
                    elif sslmode == "verify-full":
                        ssl_context.check_hostname = True
                        self._usock = ssl_context.wrap_socket(self._usock, server_hostname=host)

                except ImportError:
                    raise InterfaceError("SSL required but ssl module not available in " "this python installation")

            self._sock: typing.Optional[typing.BinaryIO] = self._usock.makefile(mode="rwb")
            if tcp_keepalive:
                self._usock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except socket.error as e:
            self._usock.close()
            raise InterfaceError("communication error", e)
        self._flush: typing.Callable = self._sock.flush
        self._read: typing.Callable = self._sock.read
        self._write: typing.Callable = self._sock.write
        self._backend_key_data: typing.Optional[bytes] = None

        trans_tab = dict(zip(map(ord, "{}"), "[]"))
        glbls = {"Decimal": Decimal}

        self.inspect_funcs: typing.Dict[type, typing.Callable] = {
            Datetime: self.inspect_datetime,
            list: self.array_inspect,
            tuple: self.array_inspect,
            int: self.inspect_int,
        }

        # it's a dictionary whose key is type of message,
        # value is the corresponding function to process message
        self.message_types: typing.Dict[bytes, typing.Callable] = {
            NOTICE_RESPONSE: self.handle_NOTICE_RESPONSE,
            AUTHENTICATION_REQUEST: self.handle_AUTHENTICATION_REQUEST,
            PARAMETER_STATUS: self.handle_PARAMETER_STATUS,
            BACKEND_KEY_DATA: self.handle_BACKEND_KEY_DATA,
            READY_FOR_QUERY: self.handle_READY_FOR_QUERY,
            ROW_DESCRIPTION: self.handle_ROW_DESCRIPTION,
            ERROR_RESPONSE: self.handle_ERROR_RESPONSE,
            EMPTY_QUERY_RESPONSE: self.handle_EMPTY_QUERY_RESPONSE,
            DATA_ROW: self.handle_DATA_ROW,
            COMMAND_COMPLETE: self.handle_COMMAND_COMPLETE,
            PARSE_COMPLETE: self.handle_PARSE_COMPLETE,
            BIND_COMPLETE: self.handle_BIND_COMPLETE,
            CLOSE_COMPLETE: self.handle_CLOSE_COMPLETE,
            PORTAL_SUSPENDED: self.handle_PORTAL_SUSPENDED,
            NO_DATA: self.handle_NO_DATA,
            PARAMETER_DESCRIPTION: self.handle_PARAMETER_DESCRIPTION,
            NOTIFICATION_RESPONSE: self.handle_NOTIFICATION_RESPONSE,
            COPY_DONE: self.handle_COPY_DONE,
            COPY_DATA: self.handle_COPY_DATA,
            COPY_IN_RESPONSE: self.handle_COPY_IN_RESPONSE,
            COPY_OUT_RESPONSE: self.handle_COPY_OUT_RESPONSE,
        }

        # Int32 - Message length, including self.
        # Int32(196608) - Protocol version number.  Version 3.0.
        # Any number of key/value pairs, terminated by a zero byte:
        #   String - A parameter name (user, database, or options)
        #   String - Parameter value

        # Conduct start-up communication with database
        # Message's first part is the protocol version - Int32(196608)
        protocol: int = 196608
        val: bytearray = bytearray(i_pack(protocol))

        # Message include parameters name and value (user, database, application_name, replication)
        for k, v in init_params.items():
            val.extend(k.encode("ascii") + NULL_BYTE + typing.cast(bytes, v) + NULL_BYTE)
        val.append(0)
        # Use write and flush function to write the content of the buffer
        # and then send the message to the database
        self._write(i_pack(len(val) + 4))
        self._write(val)
        self._flush()

        self._cursor: Cursor = self.cursor()

        code = None
        self.error: typing.Optional[Exception] = None
        # When driver send the start-up message to database, DB will respond multi messages to driver
        # whose format is same with the message that driver send to DB.
        while code not in (READY_FOR_QUERY, ERROR_RESPONSE):
            # Thus use a loop to process each message
            # Each time will read 5 bytes, the first byte, the code, inform the type of message
            # following 4 bytes inform the message's length
            # then can use this length to minus 4 to get the real data.
            code, data_len = ci_unpack(self._read(5))
            self.message_types[code](self._read(data_len - 4), None)
        if self.error is not None:
            raise self.error

        # if we didn't receive a server_protocol_version from the server, default to
        # using BASE_SERVER as the server is likely lacking this functionality due to
        # being out of date
        if (
            self._client_protocol_version > ClientProtocolVersion.BASE_SERVER
            and not (b"server_protocol_version", str(self._client_protocol_version).encode()) in self.parameter_statuses
        ):
            self._client_protocol_version = ClientProtocolVersion.BASE_SERVER

        self.in_transaction = False

    @property
    def _is_multi_databases_catalog_enable_in_server(self: "Connection") -> bool:
        if (b"datashare_enabled", str("on").encode()) in self.parameter_statuses:
            return True
        else:
            # if we don't receive this param from the server, we do not support
            return False

    @property
    def is_single_database_metadata(self):
        return self._database_metadata_current_db_only or not self._is_multi_databases_catalog_enable_in_server

    def handle_ERROR_RESPONSE(self: "Connection", data, ps):
        msg: typing.Dict[str, str] = dict(
            (s[:1].decode(_client_encoding), s[1:].decode(_client_encoding)) for s in data.split(NULL_BYTE) if s != b""
        )

        response_code: str = msg[RESPONSE_CODE]
        if response_code == "28000":
            cls: type = InterfaceError
        elif response_code == "23505":
            cls = IntegrityError
        else:
            cls = ProgrammingError

        self.error = cls(msg)

    def handle_EMPTY_QUERY_RESPONSE(self: "Connection", data, ps):
        self.error = ProgrammingError("query was empty")

    def handle_CLOSE_COMPLETE(self: "Connection", data, ps):
        pass

    def handle_PARSE_COMPLETE(self: "Connection", data, ps):
        # Byte1('1') - Identifier.
        # Int32(4) - Message length, including self.
        pass

    def handle_BIND_COMPLETE(self: "Connection", data, ps):
        pass

    def handle_PORTAL_SUSPENDED(self: "Connection", data, cursor: Cursor):
        pass

    def handle_PARAMETER_DESCRIPTION(self: "Connection", data, ps):
        # Well, we don't really care -- we're going to send whatever we
        # want and let the database deal with it.  But thanks anyways!

        # count = h_unpack(data)[0]
        # type_oids = unpack_from("!" + "i" * count, data, 2)
        pass

    def handle_COPY_DONE(self: "Connection", data, ps):
        self._copy_done = True

    def handle_COPY_OUT_RESPONSE(self: "Connection", data, ps):
        # Int8(1) - 0 textual, 1 binary
        # Int16(2) - Number of columns
        # Int16(N) - Format codes for each column (0 text, 1 binary)

        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        if ps.stream is None:
            raise InterfaceError("An output stream is required for the COPY OUT response.")

    def handle_COPY_DATA(self: "Connection", data, ps) -> None:
        ps.stream.write(data)

    def handle_COPY_IN_RESPONSE(self: "Connection", data, ps):
        # Int16(2) - Number of columns
        # Int16(N) - Format codes for each column (0 text, 1 binary)
        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        if ps.stream is None:
            raise InterfaceError("An input stream is required for the COPY IN response.")

        bffr: bytearray = bytearray(8192)
        while True:
            bytes_read = ps.stream.readinto(bffr)
            if bytes_read == 0:
                break
            self._write(COPY_DATA + i_pack(bytes_read + 4))
            self._write(bffr[:bytes_read])
            self._flush()

        # Send CopyDone
        # Byte1('c') - Identifier.
        # Int32(4) - Message length, including self.
        self._write(COPY_DONE_MSG)
        self._write(SYNC_MSG)
        self._flush()

    def handle_NOTIFICATION_RESPONSE(self: "Connection", data, ps):
        ##
        # A message sent if this connection receives a NOTIFY that it was
        # LISTENing for.
        # <p>
        # Stability: When limited to accessing
        # properties from a notification event dispatch, stability is
        # guaranteed for v1.xx.
        backend_pid = i_unpack(data)[0]
        idx: int = 4
        null: int = data.find(NULL_BYTE, idx) - idx
        condition: str = data[idx : idx + null].decode("ascii")
        idx += null + 1
        null = data.find(NULL_BYTE, idx) - idx
        # additional_info = data[idx:idx + null]

        self.notifications.append((backend_pid, condition))

    def cursor(self: "Connection") -> Cursor:
        """Creates a :class:`Cursor` object bound to this
        connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        return Cursor(self)

    @property
    def description(self: "Connection") -> typing.Optional[typing.List]:
        return self._run_cursor._getDescription()

    def run(self: "Connection", sql, stream=None, **params):
        self._run_cursor.execute(sql, params, stream=stream)
        return tuple(self._run_cursor._cached_rows)

    def commit(self: "Connection") -> None:
        """Commits the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self.execute(self._cursor, "commit", None)

    def rollback(self: "Connection") -> None:
        """Rolls back the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if not self.in_transaction:
            return
        self.execute(self._cursor, "rollback", None)

    def close(self: "Connection") -> None:
        """Closes the database connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        try:
            # Byte1('X') - Identifies the message as a terminate message.
            # Int32(4) - Message length, including self.
            self._write(TERMINATE_MSG)
            self._flush()
            if self._sock is not None:
                self._sock.close()
        except AttributeError:
            raise InterfaceError("connection is closed")
        except ValueError:
            raise InterfaceError("connection is closed")
        except socket.error:
            pass
        finally:
            self._usock.close()
            self._sock = None

    def handle_AUTHENTICATION_REQUEST(self: "Connection", data: bytes, cursor: Cursor) -> None:
        # Int32 -   An authentication code that represents different
        #           authentication messages:
        #               0 = AuthenticationOk
        #               5 = MD5 pwd
        #               2 = Kerberos v5 (not supported)
        #               3 = Cleartext pwd
        #               4 = crypt() pwd (not supported)
        #               6 = SCM credential (not supported)
        #               7 = GSSAPI (not supported)
        #               8 = GSSAPI data (not supported)
        #               9 = SSPI (not supported)
        # Some authentication messages have additional data following the
        # authentication code.  That data is documented in the appropriate
        # class.
        auth_code: int = i_unpack(data)[0]
        if auth_code == 0:
            pass
        elif auth_code == 3:
            if self.password is None:
                raise InterfaceError("server requesting password authentication, but no " "password was provided")
            self._send_message(PASSWORD, self.password + NULL_BYTE)
            self._flush()
        elif auth_code == 5:
            ##
            # A message representing the backend requesting an MD5 hashed
            # password response.  The response will be sent as
            # md5(md5(pwd + login) + salt).

            # Additional message data:
            #  Byte4 - Hash salt.
            salt: bytes = b"".join(cccc_unpack(data, 4))
            if self.password is None:
                raise InterfaceError("server requesting MD5 password authentication, but no " "password was provided")
            pwd: bytes = b"md5" + md5(
                md5(self.password + self.user).hexdigest().encode("ascii") + salt
            ).hexdigest().encode("ascii")
            # Byte1('p') - Identifies the message as a password message.
            # Int32 - Message length including self.
            # String - The password.  Password may be encrypted.
            self._send_message(PASSWORD, pwd + NULL_BYTE)
            self._flush()

        elif auth_code == 10:
            # AuthenticationSASL
            mechanisms: typing.List[str] = [m.decode("ascii") for m in data[4:-1].split(NULL_BYTE)]

            self.auth: ScramClient = ScramClient(mechanisms, self.user.decode("utf8"), self.password.decode("utf8"))

            init: bytes = self.auth.get_client_first().encode("utf8")

            # SASLInitialResponse
            self._write(create_message(PASSWORD, b"SCRAM-SHA-256" + NULL_BYTE + i_pack(len(init)) + init))
            self._flush()

        elif auth_code == 11:
            # AuthenticationSASLContinue
            self.auth.set_server_first(data[4:].decode("utf8"))

            # SASLResponse
            msg: bytes = self.auth.get_client_final().encode("utf8")
            self._write(create_message(PASSWORD, msg))
            self._flush()

        elif auth_code == 12:
            # AuthenticationSASLFinal
            self.auth.set_server_final(data[4:].decode("utf8"))

        elif auth_code in (2, 4, 6, 7, 8, 9):
            raise InterfaceError("Authentication method " + str(auth_code) + " not supported by redshift_connector.")
        else:
            raise InterfaceError("Authentication method " + str(auth_code) + " not recognized by redshift_connector.")

    def handle_READY_FOR_QUERY(self: "Connection", data: bytes, ps) -> None:
        # Byte1 -   Status indicator.
        self.in_transaction = data != IDLE

    def handle_BACKEND_KEY_DATA(self: "Connection", data: bytes, ps) -> None:
        self._backend_key_data = data

    def inspect_datetime(self: "Connection", value: Datetime):
        if value.tzinfo is None:
            return py_types[1114]  # timestamp
        else:
            return py_types[1184]  # send as timestamptz

    def inspect_int(self: "Connection", value: int):
        if min_int2 < value < max_int2:
            return py_types[21]
        if min_int4 < value < max_int4:
            return py_types[23]
        if min_int8 < value < max_int8:
            return py_types[20]
        return py_types[Decimal]

    def make_params(self: "Connection", values):
        params = []
        for value in values:
            typ = type(value)
            try:
                params.append(py_types[typ])
            except KeyError:
                try:
                    params.append(self.inspect_funcs[typ](value))
                except KeyError as e:
                    param = None
                    for k, v in py_types.items():
                        try:
                            if isinstance(value, typing.cast(type, k)):
                                param = v
                                break
                        except TypeError:
                            pass

                    if param is None:
                        for k, v in self.inspect_funcs.items():  # type: ignore
                            try:
                                if isinstance(value, k):
                                    v_func: typing.Callable = typing.cast(typing.Callable, v)
                                    param = v_func(value)
                                    break
                            except TypeError:
                                pass
                            except KeyError:
                                pass

                    if param is None:
                        raise NotSupportedError("type " + str(e) + " not mapped to pg type")
                    else:
                        params.append(param)

        return tuple(params)

    # get the metadata of each row in database
    # and store these metadata into ps dictionary
    def handle_ROW_DESCRIPTION(self: "Connection", data, cursor: Cursor) -> None:
        if cursor.ps is None:
            raise InterfaceError("Cursor is missing prepared statement")
        elif "row_desc" not in cursor.ps:
            raise InterfaceError("Prepared Statement is missing row description")

        count: int = h_unpack(data)[0]
        idx = 2
        for i in range(count):
            column_label = data[idx : data.find(NULL_BYTE, idx)]
            idx += len(column_label) + 1

            field: typing.Dict = dict(
                zip(
                    ("table_oid", "column_attrnum", "type_oid", "type_size", "type_modifier", "format"),
                    ihihih_unpack(data, idx),
                )
            )
            field["label"] = column_label
            idx += 18

            if self._client_protocol_version >= ClientProtocolVersion.EXTENDED_RESULT_METADATA:
                for entry in ("schema_name", "table_name", "column_name", "catalog_name"):
                    field[entry] = data[idx : data.find(NULL_BYTE, idx)]
                    idx += len(field[entry]) + 1

                temp: int = h_unpack(data, idx)[0]
                field["nullable"] = temp & 0x1
                field["autoincrement"] = (temp >> 4) & 0x1
                field["read_only"] = (temp >> 8) & 0x1
                field["searchable"] = (temp >> 12) & 0x1
                idx += 2

            cursor.ps["row_desc"].append(field)
            field["pg8000_fc"], field["func"] = pg_types[field["type_oid"]]

    def execute(self: "Connection", cursor: Cursor, operation: str, vals) -> None:
        if vals is None:
            vals = ()

        # get the process ID of the calling process.
        pid: int = getpid()
        # multi dimensional dictionary to store the data
        # cache = self._caches[cursor.paramstyle][pid]
        # cache = {'statement': {}, 'ps': {}}
        # statement store the data of statement, ps store the data of prepared statement
        # statement = {operation(query): tuple from 'conver_paramstyle'(statement, make_args)}
        try:
            cache = self._caches[cursor.paramstyle][pid]
        except KeyError:
            try:
                param_cache = self._caches[cursor.paramstyle]
            except KeyError:
                param_cache = self._caches[cursor.paramstyle] = {}

            try:
                cache = param_cache[pid]
            except KeyError:
                cache = param_cache[pid] = {"statement": {}, "ps": {}}

        try:
            statement, make_args = cache["statement"][operation]
        except KeyError:
            statement, make_args = cache["statement"][operation] = convert_paramstyle(cursor.paramstyle, operation)

        args = make_args(vals)
        # change the args to the format that the DB will identify
        # take reference from self.py_types
        params = self.make_params(args)
        key = operation, params

        try:
            ps = cache["ps"][key]
            cursor.ps = ps
        except KeyError:
            statement_nums: typing.List[int] = [0]
            for style_cache in self._caches.values():
                try:
                    pid_cache = style_cache[pid]
                    for csh in pid_cache["ps"].values():
                        statement_nums.append(csh["statement_num"])
                except KeyError:
                    pass

            # statement_num is the id of statement increasing from 1
            statement_num: int = sorted(statement_nums)[-1] + 1
            # consist of "redshift_connector", statement, process id and statement number.
            # e.g redshift_connector_statement_11432_2
            statement_name: str = "_".join(("redshift_connector", "statement", str(pid), str(statement_num)))
            statement_name_bin: bytes = statement_name.encode("ascii") + NULL_BYTE
            # row_desc: list that used to store metadata of rows from DB
            # param_funcs: type transform function
            ps = {
                "statement_name_bin": statement_name_bin,
                "pid": pid,
                "statement_num": statement_num,
                "row_desc": [],
                "param_funcs": tuple(x[2] for x in params),
            }
            cursor.ps = ps

            param_fcs = tuple(x[1] for x in params)

            # Byte1('P') - Identifies the message as a Parse command.
            # Int32 -   Message length, including self.
            # String -  Prepared statement name. An empty string selects the
            #           unnamed prepared statement.
            # String -  The query string.
            # Int16 -   Number of parameter data types specified (can be zero).
            # For each parameter:
            #   Int32 - The OID of the parameter data type.
            val: typing.Union[bytes, bytearray] = bytearray(statement_name_bin)
            typing.cast(bytearray, val).extend(statement.encode(_client_encoding) + NULL_BYTE)
            typing.cast(bytearray, val).extend(h_pack(len(params)))
            for oid, fc, send_func in params:
                # Parse message doesn't seem to handle the -1 type_oid for NULL
                # values that other messages handle.  So we'll provide type_oid
                # 705, the PG "unknown" type.
                typing.cast(bytearray, val).extend(i_pack(705 if oid == -1 else oid))

            # Byte1('D') - Identifies the message as a describe command.
            # Int32 - Message length, including self.
            # Byte1 - 'S' for prepared statement, 'P' for portal.
            # String - The name of the item to describe.

            # PARSE message will notify database to create a prepared statement object
            self._send_message(PARSE, val)
            # DESCRIBE message will specify the name of the existing prepared statement
            # the response will be a parameterDescribing message describe the parameters needed
            # and a RowDescription message describe the rows will be return(nodata message when no return rows)
            self._send_message(DESCRIBE, STATEMENT + statement_name_bin)
            # at completion of query message, driver issue a sync message
            self._write(SYNC_MSG)

            try:
                self._flush()
            except AttributeError as e:
                if self._sock is None:
                    raise InterfaceError("connection is closed")
                else:
                    raise e

            self.handle_messages(cursor)

            # We've got row_desc that allows us to identify what we're
            # going to get back from this statement.
            output_fc = tuple(pg_types[f["type_oid"]][0] for f in ps["row_desc"])

            ps["input_funcs"] = tuple(f["func"] for f in ps["row_desc"])
            # Byte1('B') - Identifies the Bind command.
            # Int32 - Message length, including self.
            # String - Name of the destination portal.
            # String - Name of the source prepared statement.
            # Int16 - Number of parameter format codes.
            # For each parameter format code:
            #   Int16 - The parameter format code.
            # Int16 - Number of parameter values.
            # For each parameter value:
            #   Int32 - The length of the parameter value, in bytes, not
            #           including this length.  -1 indicates a NULL parameter
            #           value, in which no value bytes follow.
            #   Byte[n] - Value of the parameter.
            # Int16 - The number of result-column format codes.
            # For each result-column format code:
            #   Int16 - The format code.
            ps["bind_1"] = (
                NULL_BYTE
                + statement_name_bin
                + h_pack(len(params))
                + pack("!" + "h" * len(param_fcs), *param_fcs)
                + h_pack(len(params))
            )

            ps["bind_2"] = h_pack(len(output_fc)) + pack("!" + "h" * len(output_fc), *output_fc)

            if len(cache["ps"]) > self.max_prepared_statements:
                for p in cache["ps"].values():
                    self.close_prepared_statement(p["statement_name_bin"])
                cache["ps"].clear()

            cache["ps"][key] = ps

        cursor._cached_rows.clear()
        cursor._row_count = -1

        # Byte1('B') - Identifies the Bind command.
        # Int32 - Message length, including self.
        # String - Name of the destination portal.
        # String - Name of the source prepared statement.
        # Int16 - Number of parameter format codes.
        # For each parameter format code:
        #   Int16 - The parameter format code.
        # Int16 - Number of parameter values.
        # For each parameter value:
        #   Int32 - The length of the parameter value, in bytes, not
        #           including this length.  -1 indicates a NULL parameter
        #           value, in which no value bytes follow.
        #   Byte[n] - Value of the parameter.
        # Int16 - The number of result-column format codes.
        # For each result-column format code:
        #   Int16 - The format code.
        retval: bytearray = bytearray(ps["bind_1"])
        for value, send_func in zip(args, ps["param_funcs"]):
            if value is None:
                val = NULL
            else:
                val = send_func(value)
                retval.extend(i_pack(len(val)))
            retval.extend(val)
        retval.extend(ps["bind_2"])

        # send BIND message which includes name of parepared statement,
        # name of destination portal and the value of placeholders in prepared statement.
        # these parameters need to match the prepared statements
        self._send_message(BIND, retval)
        self.send_EXECUTE(cursor)
        self._write(SYNC_MSG)
        self._flush()
        # handle multi messages including BIND_COMPLETE, DATA_ROW, COMMAND_COMPLETE
        # READY_FOR_QUERY
        if self.merge_socket_read:
            self.handle_messages_merge_socket_read(cursor)
        else:
            self.handle_messages(cursor)

    def _send_message(self: "Connection", code: bytes, data: bytes) -> None:
        try:
            self._write(code)
            self._write(i_pack(len(data) + 4))
            self._write(data)
            self._write(FLUSH_MSG)
        except ValueError as e:
            if str(e) == "write to closed file":
                raise InterfaceError("connection is closed")
            else:
                raise e
        except AttributeError:
            raise InterfaceError("connection is closed")

    def send_EXECUTE(self: "Connection", cursor: Cursor) -> None:
        # Byte1('E') - Identifies the message as an execute message.
        # Int32 -   Message length, including self.
        # String -  The name of the portal to execute.
        # Int32 -   Maximum number of rows to return, if portal
        #           contains a query # that returns rows.
        #           0 = no limit.
        self._write(EXECUTE_MSG)
        self._write(FLUSH_MSG)

    def handle_NO_DATA(self: "Connection", msg, ps) -> None:
        pass

    # Handle the command complete message
    # consists of b'C', length of message in bytes
    # and the command tag which is a single number identify
    # how many SQL commands was completed
    # not support for 'SELECT' and 'COPY' query
    def handle_COMMAND_COMPLETE(self: "Connection", data: bytes, cursor: Cursor) -> None:
        values: typing.List[bytes] = data[:-1].split(b" ")
        command = values[0]
        if command in self._commands_with_count:
            row_count: int = int(values[-1])
            if cursor._row_count == -1:
                cursor._row_count = row_count
            else:
                cursor._row_count += row_count

        if command in (b"ALTER", b"CREATE"):
            for scache in self._caches.values():
                for pcache in scache.values():
                    for ps in pcache["ps"].values():
                        self.close_prepared_statement(ps["statement_name_bin"])
                    pcache["ps"].clear()

    # Get the data respond from database
    # transfrom the Redshift data tye into python data type
    # store the data into _cached_rows
    def handle_DATA_ROW(self: "Connection", data: bytes, cursor: Cursor) -> None:
        data_idx: int = 2
        row: typing.List = []
        for func in cursor.ps["input_funcs"]:  # type: ignore
            vlen: int = i_unpack(data, data_idx)[0]
            data_idx += 4
            if vlen == -1:
                row.append(None)
            else:
                row.append(func(data, data_idx, vlen))
                data_idx += vlen
        cursor._cached_rows.append(row)

    def handle_messages(self: "Connection", cursor: Cursor) -> None:
        code = self.error = None

        while code != READY_FOR_QUERY:
            code, data_len = ci_unpack(self._read(5))
            self.message_types[code](self._read(data_len - 4), cursor)

        if self.error is not None:
            raise self.error

    def handle_messages_merge_socket_read(self: "Connection", cursor: Cursor):
        code = self.error = None
        # read 5 bytes of message firstly
        code, data_len = ci_unpack(self._read(5))

        while True:
            if code == READY_FOR_QUERY:
                # for last message
                self.message_types[code](self._read(data_len - 4), cursor)
                break
            else:
                # read data body of last message and read next 5 bytes of next message
                data = self._read(data_len - 4 + 5)
                last_message_body = data[0:-5]
                self.message_types[code](last_message_body, cursor)
                code, data_len = ci_unpack(data[-5:])

        if self.error is not None:
            raise self.error

    # Byte1('C') - Identifies the message as a close command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def close_prepared_statement(self: "Connection", statement_name_bin: bytes) -> None:
        self._send_message(CLOSE, STATEMENT + statement_name_bin)
        self._write(SYNC_MSG)
        self._flush()
        self.handle_messages(self._cursor)

    # Byte1('N') - Identifier
    # Int32 - Message length
    # Any number of these, followed by a zero byte:
    #   Byte1 - code identifying the field type (see responseKeys)
    #   String - field value
    def handle_NOTICE_RESPONSE(self: "Connection", data: bytes, ps) -> None:
        self.notices.append(dict((s[0:1], s[1:]) for s in data.split(NULL_BYTE)))

    # Handle the parameters status from database including version, time, data type
    # need to be noticed that the redshift based on the version 8 of PostgreSql
    def handle_PARAMETER_STATUS(self: "Connection", data: bytes, ps) -> None:
        pos: int = data.find(NULL_BYTE)
        key, value = data[:pos], data[pos + 1 : -1]
        self.parameter_statuses.append((key, value))
        if key == b"client_encoding":
            encoding = value.decode("ascii").lower()
            _client_encoding = pg_to_py_encodings.get(encoding, encoding)
        elif key == b"server_protocol_version":
            # when a mismatch occurs between the client's requested protocol version, and the server's response,
            # warn the user and follow server
            if self._client_protocol_version != int(value):
                warn(
                    "Server indicated {} transfer protocol will be used rather than protocol requested by client: {}".format(
                        ClientProtocolVersion.get_name(int(value)),
                        ClientProtocolVersion.get_name(self._client_protocol_version),
                    ),
                    stacklevel=3,
                )
                self._client_protocol_version = int(value)
        elif key == b"server_version":
            self._server_version: LooseVersion = LooseVersion(value.decode("ascii"))
            if self._server_version < LooseVersion("8.2.0"):
                self._commands_with_count = (b"INSERT", b"DELETE", b"UPDATE", b"MOVE")
            elif self._server_version < LooseVersion("9.0.0"):
                self._commands_with_count = (b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH", b"COPY")

    def array_inspect(self: "Connection", value):
        # Check if array has any values. If empty, we can just assume it's an
        # array of strings
        first_element = array_find_first_element(value)
        if first_element is None:
            oid: int = 25
            # Use binary ARRAY format to avoid having to properly
            # escape text in the array literals
            fc: int = FC_BINARY
            array_oid: int = pg_array_types[oid]
        else:
            # supported array output
            typ: type = type(first_element)

            if issubclass(typ, int):
                # special int array support -- send as smallest possible array
                # type
                typ = int
                int2_ok, int4_ok, int8_ok = True, True, True
                for v in array_flatten(value):
                    if v is None:
                        continue
                    if min_int2 < v < max_int2:
                        continue
                    int2_ok = False
                    if min_int4 < v < max_int4:
                        continue
                    int4_ok = False
                    if min_int8 < v < max_int8:
                        continue
                    int8_ok = False
                if int2_ok:
                    array_oid = 1005  # INT2[]
                    oid, fc, send_func = (21, FC_BINARY, h_pack)
                elif int4_ok:
                    array_oid = 1007  # INT4[]
                    oid, fc, send_func = (23, FC_BINARY, i_pack)
                elif int8_ok:
                    array_oid = 1016  # INT8[]
                    oid, fc, send_func = (20, FC_BINARY, q_pack)
                else:
                    raise ArrayContentNotSupportedError("numeric not supported as array contents")
            else:
                try:
                    oid, fc, send_func = self.make_params((first_element,))[0]

                    # If unknown or string, assume it's a string array
                    if oid in (705, 1043, 25):
                        oid = 25
                        # Use binary ARRAY format to avoid having to properly
                        # escape text in the array literals
                        fc = FC_BINARY
                    array_oid = pg_array_types[oid]
                except KeyError:
                    raise ArrayContentNotSupportedError("oid " + str(oid) + " not supported as array contents")
                except NotSupportedError:
                    raise ArrayContentNotSupportedError("type " + str(typ) + " not supported as array contents")
        if fc == FC_BINARY:

            def send_array(arr: typing.List) -> typing.Union[bytes, bytearray]:
                # check that all array dimensions are consistent
                array_check_dimensions(arr)

                has_null: bool = array_has_null(arr)
                dim_lengths: typing.List[int] = array_dim_lengths(arr)
                data: bytearray = bytearray(iii_pack(len(dim_lengths), has_null, oid))
                for i in dim_lengths:
                    data.extend(ii_pack(i, 1))
                for v in array_flatten(arr):
                    if v is None:
                        data += i_pack(-1)
                    elif isinstance(v, typ):
                        inner_data = send_func(v)
                        data += i_pack(len(inner_data))
                        data += inner_data
                    else:
                        raise ArrayContentNotHomogenousError("not all array elements are of type " + str(typ))
                return data

        else:

            def send_array(arr: typing.List) -> typing.Union[bytes, bytearray]:
                array_check_dimensions(arr)
                ar: typing.List = deepcopy(arr)
                for a, i, v in walk_array(ar):
                    if v is None:
                        a[i] = "NULL"
                    elif isinstance(v, typ):
                        a[i] = send_func(v).decode("ascii")
                    else:
                        raise ArrayContentNotHomogenousError("not all array elements are of type " + str(typ))
                return str(ar).translate(arr_trans).encode("ascii")

        return (array_oid, fc, send_array)

    def xid(self: "Connection", format_id, global_transaction_id, branch_qualifier):
        """Create a Transaction IDs (only global_transaction_id is used in pg)
        format_id and branch_qualifier are not used in postgres
        global_transaction_id may be any string identifier supported by
        postgres returns a tuple
        (format_id, global_transaction_id, branch_qualifier)"""
        return (format_id, global_transaction_id, branch_qualifier)

    def tpc_begin(self: "Connection", xid) -> None:
        """Begins a TPC transaction with the given transaction ID xid.

        This method should be called outside of a transaction (i.e. nothing may
        have executed since the last .commit() or .rollback()).

        Furthermore, it is an error to call .commit() or .rollback() within the
        TPC transaction. A ProgrammingError is raised, if the application calls
        .commit() or .rollback() during an active TPC transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._xid = xid
        if self.autocommit:
            self.execute(self._cursor, "begin transaction", None)

    def tpc_prepare(self: "Connection") -> None:
        """Performs the first phase of a transaction started with .tpc_begin().
        A ProgrammingError is be raised if this method is called outside of a
        TPC transaction.

        After calling .tpc_prepare(), no statements can be executed until
        .tpc_commit() or .tpc_rollback() have been called.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if self._xid is None or len(self._xid) < 2:
            raise InterfaceError("Malformed Transaction Id")

        q: str = "PREPARE TRANSACTION '%s';" % (self._xid[1],)
        self.execute(self._cursor, q, None)

    def tpc_commit(self: "Connection", xid=None) -> None:
        """When called with no arguments, .tpc_commit() commits a TPC
        transaction previously prepared with .tpc_prepare().

        If .tpc_commit() is called prior to .tpc_prepare(), a single phase
        commit is performed. A transaction manager may choose to do this if
        only a single resource is participating in the global transaction.

        When called with a transaction ID xid, the database commits the given
        transaction. If an invalid transaction ID is provided, a
        ProgrammingError will be raised. This form should be called outside of
        a transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if xid is None:
            xid = self._xid

        if xid is None:
            raise ProgrammingError("Cannot tpc_commit() without a TPC transaction!")

        try:
            previous_autocommit_mode: bool = self.autocommit
            self.autocommit = True
            if xid in self.tpc_recover():
                self.execute(self._cursor, "COMMIT PREPARED '%s';" % (xid[1],), None)
            else:
                # a single-phase commit
                self.commit()
        finally:
            self.autocommit = previous_autocommit_mode
        self._xid = None

    def tpc_rollback(self: "Connection", xid=None) -> None:
        """When called with no arguments, .tpc_rollback() rolls back a TPC
        transaction. It may be called before or after .tpc_prepare().

        When called with a transaction ID xid, it rolls back the given
        transaction. If an invalid transaction ID is provided, a
        ProgrammingError is raised. This form should be called outside of a
        transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if xid is None:
            xid = self._xid

        if xid is None:
            raise ProgrammingError("Cannot tpc_rollback() without a TPC prepared transaction!")

        try:
            previous_autocommit_mode: bool = self.autocommit
            self.autocommit = True
            if xid in self.tpc_recover():
                # a two-phase rollback
                self.execute(self._cursor, "ROLLBACK PREPARED '%s';" % (xid[1],), None)
            else:
                # a single-phase rollback
                self.rollback()
        finally:
            self.autocommit = previous_autocommit_mode
        self._xid = None

    def tpc_recover(self: "Connection"):
        """Returns a list of pending transaction IDs suitable for use with
        .tpc_commit(xid) or .tpc_rollback(xid).

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        try:
            previous_autocommit_mode: bool = self.autocommit
            self.autocommit = True
            curs = self.cursor()
            curs.execute("select xact_id FROM stl_undone")
            return [self.xid(0, row[0], "") for row in curs]
        finally:
            self.autocommit = previous_autocommit_mode
