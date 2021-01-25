import logging
import typing

from redshift_connector.config import DEFAULT_PROTOCOL_VERSION
from redshift_connector.core import BINARY, Connection, Cursor
from redshift_connector.error import (
    ArrayContentNotHomogenousError,
    ArrayContentNotSupportedError,
    ArrayDimensionsNotConsistentError,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from redshift_connector.iam_helper import set_iam_properties
from redshift_connector.objects import (
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from redshift_connector.pg_types import (
    PGEnum,
    PGJson,
    PGJsonb,
    PGText,
    PGTsvector,
    PGVarchar,
)
from redshift_connector.redshift_property import RedshiftProperty

from .version import __version__

logging.getLogger(__name__).addHandler(logging.NullHandler())

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


def connect(
    user: str,
    database: str,
    password: str,
    port: int = 5439,
    host: str = "localhost",
    source_address: typing.Optional[str] = None,
    unix_sock: typing.Optional[str] = None,
    ssl: bool = True,
    sslmode: str = "verify-ca",
    timeout: typing.Optional[int] = None,
    max_prepared_statements: int = 1000,
    tcp_keepalive: bool = True,
    application_name: typing.Optional[str] = None,
    replication: typing.Optional[str] = None,
    idp_host: typing.Optional[str] = None,
    db_user: typing.Optional[str] = None,
    app_id: typing.Optional[str] = None,
    app_name: str = "amazon_aws_redshift",
    preferred_role: typing.Optional[str] = None,
    principal_arn: typing.Optional[str] = None,
    access_key_id: typing.Optional[str] = None,
    secret_access_key: typing.Optional[str] = None,
    session_token: typing.Optional[str] = None,
    profile: typing.Optional[str] = None,
    credentials_provider: typing.Optional[str] = None,
    region: typing.Optional[str] = None,
    cluster_identifier: typing.Optional[str] = None,
    iam: bool = False,
    client_id: typing.Optional[str] = None,
    idp_tenant: typing.Optional[str] = None,
    client_secret: typing.Optional[str] = None,
    partner_sp_id: typing.Optional[str] = None,
    idp_response_timeout: int = 120,
    listen_port: int = 7890,
    login_url: typing.Optional[str] = None,
    auto_create: bool = False,
    db_groups: typing.List[str] = list(),
    force_lowercase: bool = False,
    allow_db_user_override: bool = False,
    client_protocol_version: int = DEFAULT_PROTOCOL_VERSION,
    database_metadata_current_db_only: bool = True,
    ssl_insecure: typing.Optional[bool] = None,
) -> Connection:

    info: RedshiftProperty = RedshiftProperty()
    set_iam_properties(
        info,
        user=user,
        host=host,
        database=database,
        port=port,
        password=password,
        source_address=source_address,
        unix_sock=unix_sock,
        ssl=ssl,
        sslmode=sslmode,
        timeout=timeout,
        max_prepared_statements=max_prepared_statements,
        tcp_keepalive=tcp_keepalive,
        application_name=application_name,
        replication=replication,
        idp_host=idp_host,
        db_user=db_user,
        app_id=app_id,
        app_name=app_name,
        preferred_role=preferred_role,
        principal_arn=principal_arn,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        profile=profile,
        credentials_provider=credentials_provider,
        region=region,
        cluster_identifier=cluster_identifier,
        iam=iam,
        client_id=client_id,
        idp_tenant=idp_tenant,
        client_secret=client_secret,
        partner_sp_id=partner_sp_id,
        idp_response_timeout=idp_response_timeout,
        listen_port=listen_port,
        login_url=login_url,
        auto_create=auto_create,
        db_groups=db_groups,
        force_lowercase=force_lowercase,
        allow_db_user_override=allow_db_user_override,
        client_protocol_version=client_protocol_version,
        database_metadata_current_db_only=database_metadata_current_db_only,
        ssl_insecure=ssl_insecure,
    )

    return Connection(
        user=info.user_name,
        host=info.host,
        database=info.db_name,
        port=info.port,
        password=info.password,
        source_address=info.source_address,
        unix_sock=info.unix_sock,
        ssl=info.ssl,
        sslmode=info.sslmode,
        timeout=info.timeout,
        max_prepared_statements=info.max_prepared_statements,
        tcp_keepalive=info.tcp_keepalive,
        application_name=info.application_name,
        replication=info.replication,
        client_protocol_version=info.client_protocol_version,
        database_metadata_current_db_only=database_metadata_current_db_only,
    )


apilevel: str = "2.0"
"""The DBAPI level supported, currently "2.0".

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

threadsafety: int = 1
"""Integer constant stating the level of thread safety the DBAPI interface
supports. This DBAPI module supports sharing of the module only. Connections
and cursors my not be shared between threads.

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

paramstyle: str = "format"

# I have no idea what this would be used for by a client app.  Should it be
# TEXT, VARCHAR, CHAR?  It will only compare against row_description's
# type_code if it is this one type.  It is the varchar type oid for now, this
# appears to match expectations in the DB API 2.0 compliance test suite.

STRING: int = 1043
"""String type oid."""


NUMBER: int = 1700
"""Numeric type oid"""

DATETIME: int = 1114
"""Timestamp type oid"""

ROWID: int = 26
"""ROWID type oid"""

__all__: typing.Any = [
    "Warning",
    "DataError",
    "DatabaseError",
    "connect",
    "InterfaceError",
    "ProgrammingError",
    "Error",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "NotSupportedError",
    "ArrayContentNotHomogenousError",
    "ArrayDimensionsNotConsistentError",
    "ArrayContentNotSupportedError",
    "Connection",
    "Cursor",
    "Binary",
    "Date",
    "DateFromTicks",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "BINARY",
    "PGEnum",
    "PGJson",
    "PGJsonb",
    "PGTsvector",
    "PGText",
    "PGVarchar",
]
