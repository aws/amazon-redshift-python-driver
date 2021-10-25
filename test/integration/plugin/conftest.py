import typing
from test.conftest import _get_default_connection_args, conf

import pytest

import redshift_connector


@pytest.fixture(scope="session", autouse=True)
def startup_db_stmts():
    """
    Executes a defined set of statements to configure a fresh Amazon Redshift cluster for IdP integration tests.
    """
    groups: typing.List[str] = conf.get("cluster-setup", "groups").split(sep=",")

    with redshift_connector.connect(**_get_default_connection_args()) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for grp in groups:
                try:
                    cursor.execute("DROP GROUP {}".format(grp))
                except:
                    pass  # we can't use IF EXISTS here, so ignore any error
                cursor.execute("CREATE GROUP {}".format(grp))
