import configparser
import logging
import os
import random
import string
import sys
import typing

import pytest  # type: ignore

import redshift_connector
from redshift_connector import DriverInfo
from redshift_connector.config import ClientProtocolVersion

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

logger = logging.getLogger(__name__)

# Check if running in Jython
if "java" in sys.platform:
    from jarray import array  # type: ignore
    from javax.net.ssl import SSLContext, TrustManager, X509TrustManager  # type: ignore

    class TrustAllX509TrustManager(X509TrustManager):
        """Define a custom TrustManager which will blindly accept all
        certificates"""

        def checkClientTrusted(self, chain, auth):
            pass

        def checkServerTrusted(self, chain, auth):
            pass

        def getAcceptedIssuers(self):
            return None

    # Create a static reference to an SSLContext which will use
    # our custom TrustManager
    trust_managers = array([TrustAllX509TrustManager()], TrustManager)
    TRUST_ALL_CONTEXT = SSLContext.getInstance("SSL")
    TRUST_ALL_CONTEXT.init(None, trust_managers, None)
    # Keep a static reference to the JVM's default SSLContext for restoring
    # at a later time
    DEFAULT_CONTEXT = SSLContext.getDefault()


@pytest.fixture
def trust_all_certificates(request) -> None:
    """Decorator function that will make it so the context of the decorated
    method will run with our TrustManager that accepts all certificates"""
    # Only do this if running under Jython
    is_java: bool = "java" in sys.platform

    if is_java:
        from javax.net.ssl import SSLContext

        SSLContext.setDefault(TRUST_ALL_CONTEXT)

    def fin():
        if is_java:
            SSLContext.setDefault(DEFAULT_CONTEXT)

    request.addfinalizer(fin)


def test_socket_missing(db_kwargs) -> None:
    db_kwargs["unix_sock"] = "/file-does-not-exist"

    with pytest.raises(redshift_connector.InterfaceError):
        redshift_connector.connect(**db_kwargs)


def test_database_missing(db_kwargs) -> None:
    db_kwargs["database"] = "missing-db"
    with pytest.raises(redshift_connector.ProgrammingError):
        redshift_connector.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires md5 for the database
# redshift_connector_md5


def test_md5(db_kwargs) -> None:
    db_kwargs["database"] = "redshift_connector_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


@pytest.mark.parametrize("algorithm", ("sha256",))
def test_auth_req_digest(db_kwargs, algorithm) -> None:
    test_user: str = "{}_dbuser".format(algorithm)
    test_password: str = "My_{}_PaSsWoRdŽ".format(algorithm)
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("drop user if exists {}".format(test_user))
            cursor.execute(
                "create user {} with password '{}'".format(test_user, "{}|{}".format(algorithm, test_password))
            )
            conn.commit()
    try:
        with redshift_connector.connect(**{**db_kwargs, **{"user": test_user, "password": test_password}}) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select 1")
    except:
        raise
    finally:
        with redshift_connector.connect(**db_kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute("drop user if exists {}".format(test_user))
                conn.commit()


@pytest.mark.usefixtures("trust_all_certificates")
def test_ssl(db_kwargs) -> None:
    db_kwargs["ssl"] = True
    db_kwargs["sslmode"] = "verify-ca"
    with redshift_connector.connect(**db_kwargs):
        pass
    db_kwargs["sslmode"] = "verify-full"
    with redshift_connector.connect(**db_kwargs):
        pass


# This requires a line in pg_hba.conf that requires 'password' for the
# database redshift_connector_password
def test_password(db_kwargs) -> None:
    db_kwargs["database"] = "redshift_connector_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


def test_unicode_database_name(db_kwargs) -> None:
    db_kwargs["database"] = "redshift_connector_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


def test_bytes_database_name(db_kwargs) -> None:
    """Should only raise an exception saying db doesn't exist"""

    db_kwargs["database"] = bytes("redshift_connector_sn\uFF6Fw", "utf8")
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


def test_bytes_password(con, db_kwargs) -> None:
    # Create user
    username: str = "boltzman"
    password: str = "C1cccccha\uFF6Fs"
    with con.cursor() as cur:
        cur.execute("drop user if exists {};".format(username))
        cur.execute("create user {} with password '{}';".format(username, password))
        con.commit()

        db_kwargs["user"] = username
        db_kwargs["password"] = password.encode("utf8")
        db_kwargs["database"] = "redshift_connector_md5"
        with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
            redshift_connector.connect(**db_kwargs)

        cur.execute("drop user {}".format(username))
        con.commit()


def test_broken_pipe(con, db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as db1:
        with db1.cursor() as cur1, con.cursor() as cur2:
            cur1.execute("select pg_backend_pid()")
            pid1 = typing.cast(typing.List[int], cur1.fetchone())[0]

            cur2.execute("select pg_terminate_backend(%s)", (pid1,))
            with pytest.raises(
                redshift_connector.InterfaceError,
                match="BrokenPipe: server socket closed. Please check that client side networking configurations such "
                "as Proxies, firewalls, VPN, etc. are not affecting your network connection.",
            ):
                cur1.execute("select 1")


# case 2: same connector configuration, but should throw an error since the timeout is set,
def test_broken_pipe_timeout(con, db_kwargs) -> None:
    db_kwargs["timeout"] = 60
    with redshift_connector.connect(**db_kwargs) as db1:
        with db1.cursor() as cur1, con.cursor() as cur2:
            print(db1._usock.timeout)
            cur1.execute("select pg_backend_pid()")
            pid1 = typing.cast(typing.List[int], cur1.fetchone())[0]

            cur2.execute("select pg_terminate_backend(%s)", (pid1,))
            with pytest.raises(
                redshift_connector.InterfaceError,
                match="BrokenPipe: server socket closed. We noticed a timeout is "
                "set for this connection. Consider raising the timeout or "
                "defaulting timeout to none.",
            ):
                cur1.execute("select 1")


def test_application_name_integer(db_kwargs) -> None:
    db_kwargs["application_name"] = 1
    with pytest.raises(
        redshift_connector.InterfaceError, match="The parameter application_name can't be of type <class 'int'>."
    ):
        redshift_connector.connect(**db_kwargs)


def test_application_name_bytearray(db_kwargs) -> None:
    db_kwargs["application_name"] = bytearray(b"Philby")
    redshift_connector.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256


def test_scram_sha_256(db_kwargs) -> None:
    db_kwargs["database"] = "redshift_connector_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


@pytest.mark.parametrize("_input", ClientProtocolVersion.list()[:-1])
def test_client_protocol_version_is_used(db_kwargs, _input) -> None:
    db_kwargs["client_protocol_version"] = _input

    with redshift_connector.connect(**db_kwargs) as conn:
        assert conn._client_protocol_version == _input


# def test_client_protocol_version_invalid_logs(db_kwargs, caplog):
#     db_kwargs["client_protocol_version"] = max(ClientProtocolVersion.list()) + 1
#     del db_kwargs["region"]
#     del db_kwargs["cluster_identifier"]
#
#     with caplog.at_level(logging.DEBUG):
#         with redshift_connector.Connection(**db_kwargs) as con:
#             act_client_protocol: int = con._client_protocol_version
#     assert "Server indicated {} transfer protocol will be used rather than protocol requested by client: {}".format(
#         act_client_protocol, db_kwargs["client_protocol_version"]
#     )


def test_client_protocol_version_too_large_is_lowered(db_kwargs, mocker) -> None:
    db_kwargs["client_protocol_version"] = max(ClientProtocolVersion.list()) + 1
    del db_kwargs["region"]
    del db_kwargs["cluster_identifier"]
    spy = mocker.spy(redshift_connector.Connection, "_enable_protocol_based_conversion_funcs")
    with redshift_connector.Connection(**db_kwargs) as con:
        act_client_protocol: int = con._client_protocol_version
    assert act_client_protocol < db_kwargs["client_protocol_version"]
    assert spy.called
    assert spy.call_count >= 2  # initial call, additional call when server responds with a lower version


def test_stl_connection_log_contains_driver_version(db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            # verify stl_connection_log contains driver version as expected
            cursor.execute(
                "select top 1 1 from stl_connection_log where driver_version = '{}'".format(
                    DriverInfo.driver_full_name()
                )
            )
            res = cursor.fetchone()
            assert res is not None
            assert res[0] == 1


def test_stl_connection_log_contains_os_version(db_kwargs) -> None:
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            # verify stl_connection_log contains driver version as expected
            cursor.execute(
                "select top 1 1 from stl_connection_log where driver_version = '{}' and os_version = '{}'".format(
                    DriverInfo.driver_full_name(), conn.client_os_version
                )
            )
            res = cursor.fetchone()
            assert res is not None
            assert res[0] == 1


def test_stl_connection_log_contains_application_name(db_kwargs) -> None:
    # make some connection so this unique application name is logged
    mock_application_name: str = "".join(random.choice(string.ascii_letters) for x in range(10))
    db_kwargs["application_name"] = mock_application_name

    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            # verify stl_connection_log contains driver version as expected
            cursor.execute(
                "select top 1 1 from stl_connection_log where driver_version = '{}' and application_name = '{}'".format(
                    DriverInfo.driver_full_name(), mock_application_name
                )
            )
            res = cursor.fetchone()
            assert res is not None
            assert res[0] == 1


@pytest.mark.parametrize("sql", ["select 1", "grant role sys:monitor to awsuser"])
def test_execute_skip_parse_bind_params_when_dne(mocker, db_kwargs, sql) -> None:
    convert_paramstyle_spy = mocker.spy(redshift_connector.core, "convert_paramstyle")

    with redshift_connector.connect(**db_kwargs) as conn:
        conn.cursor().execute(sql)
    assert not convert_paramstyle_spy.called


@pytest.mark.parametrize(
    "sql, args",
    [("select %s", "hello world"), ("select %s, %s", ("hello", "world")), ("select %s, %s", [1, "hello world"])],
)
def test_execute_do_parsing_bind_params_when_exist(mocker, db_kwargs, sql, args) -> None:
    convert_paramstyle_spy = mocker.spy(redshift_connector.core, "convert_paramstyle")

    with redshift_connector.connect(**db_kwargs) as conn:
        conn.cursor().execute(sql, args)
    assert convert_paramstyle_spy.called


def test_socket_timeout(db_kwargs) -> None:
    db_kwargs["timeout"] = 0

    with pytest.raises(redshift_connector.InterfaceError):
        redshift_connector.connect(**db_kwargs)
