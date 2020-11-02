import configparser
import os
import socket
import struct
import sys

import pytest  # type: ignore

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")

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
def trust_all_certificates(request):
    """Decorator function that will make it so the context of the decorated
    method will run with our TrustManager that accepts all certificates"""
    # Only do this if running under Jython
    is_java = "java" in sys.platform

    if is_java:
        from javax.net.ssl import SSLContext

        SSLContext.setDefault(TRUST_ALL_CONTEXT)

    def fin():
        if is_java:
            SSLContext.setDefault(DEFAULT_CONTEXT)

    request.addfinalizer(fin)


def testSocketMissing():
    conn_params = {
        "unix_sock": "/file-does-not-exist",
        "user": "doesn't-matter",
        "password": "hunter2",
        "database": "myDb",
    }

    with pytest.raises(redshift_connector.InterfaceError):
        redshift_connector.connect(**conn_params)


def testDatabaseMissing(db_kwargs):
    db_kwargs["database"] = "missing-db"
    with pytest.raises(redshift_connector.ProgrammingError):
        redshift_connector.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires md5 for the database
# redshift_connector_md5


def testMd5(db_kwargs):
    db_kwargs["database"] = "redshift_connector_md5"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


@pytest.mark.usefixtures("trust_all_certificates")
def testSsl(db_kwargs):
    db_kwargs["ssl"] = True
    db_kwargs["sslmode"] = "verify-ca"
    with redshift_connector.connect(**db_kwargs):
        pass
    db_kwargs["sslmode"] = "verify-full"
    with redshift_connector.connect(**db_kwargs):
        pass


# This requires a line in pg_hba.conf that requires 'password' for the
# database redshift_connector_password
def testPassword(db_kwargs):
    db_kwargs["database"] = "redshift_connector_password"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


def testUnicodeDatabaseName(db_kwargs):
    db_kwargs["database"] = "redshift_connector_sn\uFF6Fw"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


def testBytesDatabaseName(db_kwargs):
    """ Should only raise an exception saying db doesn't exist """

    db_kwargs["database"] = bytes("redshift_connector_sn\uFF6Fw", "utf8")
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)


def testBytesPassword(con, db_kwargs):
    # Create user
    username = "boltzman"
    password = "C1cccccha\uFF6Fs"
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


def test_broken_pipe(con, db_kwargs):
    with redshift_connector.connect(**db_kwargs) as db1:
        with db1.cursor() as cur1, con.cursor() as cur2:
            cur1.execute("select pg_backend_pid()")
            pid1 = cur1.fetchone()[0]

            cur2.execute("select pg_terminate_backend(%s)", (pid1,))
            try:
                cur1.execute("select 1")
            except Exception as e:
                assert isinstance(e, (socket.error, struct.error))


def test_application_name_integer(db_kwargs):
    db_kwargs["application_name"] = 1
    with pytest.raises(
        redshift_connector.InterfaceError, match="The parameter application_name can't be of type " "<class 'int'>."
    ):
        redshift_connector.connect(**db_kwargs)


def test_application_name_bytearray(db_kwargs):
    db_kwargs["application_name"] = bytearray(b"Philby")
    redshift_connector.connect(**db_kwargs)


# This requires a line in pg_hba.conf that requires scram-sha-256 for the
# database scram-sha-256


def test_scram_sha_256(db_kwargs):
    db_kwargs["database"] = "redshift_connector_scram_sha_256"

    # Should only raise an exception saying db doesn't exist
    with pytest.raises(redshift_connector.ProgrammingError, match="3D000"):
        redshift_connector.connect(**db_kwargs)
