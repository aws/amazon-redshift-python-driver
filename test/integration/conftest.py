import configparser
import os
import sys

import pytest  # type: ignore

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
conf.read(root_path + "/config.ini")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # we only look at actual failing test calls, not setup/teardown
    if rep.when == "call" and rep.failed:
        mode = "a" if os.path.exists("failures") else "w"
        with open("failures", mode) as f:
            f.write(rep.longreprtext + "\n")


@pytest.fixture(scope="class")
def db_kwargs():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "user": conf.get("database", "user"),
        "password": conf.get("database", "password"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
    }

    return db_connect


@pytest.fixture(scope="class")
def okta_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "password": conf.get("okta-idp", "password"),
        "iam": conf.getboolean("okta-idp", "iam"),
        "idp_host": conf.get("okta-idp", "idp_host"),
        "user": conf.get("okta-idp", "user"),
        "app_id": conf.get("okta-idp", "app_id"),
        "app_name": conf.get("okta-idp", "app_name"),
        "credentials_provider": conf.get("okta-idp", "credentials_provider"),
        "region": conf.get("okta-idp", "region"),
        "cluster_identifier": conf.get("okta-idp", "cluster_identifier"),
    }
    return db_connect


@pytest.fixture(scope="class")
def okta_browser_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "password": conf.get("okta-browser-idp", "password"),
        "iam": conf.getboolean("okta-browser-idp", "iam"),
        "user": conf.get("okta-browser-idp", "user"),
        "credentials_provider": conf.get("okta-browser-idp", "credentials_provider"),
        "region": conf.get("okta-browser-idp", "region"),
        "cluster_identifier": conf.get("okta-browser-idp", "cluster_identifier"),
        "login_url": conf.get("okta-browser-idp", "login_url"),
    }
    return db_connect


@pytest.fixture(scope="class")
def azure_browser_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "password": conf.get("azure-browser-idp", "password"),
        "iam": conf.getboolean("azure-browser-idp", "iam"),
        "user": conf.get("azure-browser-idp", "user"),
        "credentials_provider": conf.get("azure-browser-idp", "credentials_provider"),
        "region": conf.get("azure-browser-idp", "region"),
        "cluster_identifier": conf.get("azure-browser-idp", "cluster_identifier"),
        "idp_tenant": conf.get("azure-browser-idp", "idp_tenant"),
        "client_id": conf.get("azure-browser-idp", "client_id"),
        "client_secret": conf.get("azure-browser-idp", "client_secret"),
    }
    return db_connect


@pytest.fixture(scope="class")
def azure_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "password": conf.get("azure-idp", "password"),
        "iam": conf.getboolean("azure-idp", "iam"),
        "user": conf.get("azure-idp", "user"),
        "credentials_provider": conf.get("azure-idp", "credentials_provider"),
        "region": conf.get("azure-idp", "region"),
        "cluster_identifier": conf.get("azure-idp", "cluster_identifier"),
        "idp_tenant": conf.get("azure-idp", "idp_tenant"),
        "client_id": conf.get("azure-idp", "client_id"),
        "client_secret": conf.get("azure-idp", "client_secret"),
    }
    return db_connect


@pytest.fixture(scope="class")
def adfs_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "password": conf.get("adfs-idp", "password"),
        "iam": conf.getboolean("adfs-idp", "iam"),
        "user": conf.get("adfs-idp", "user"),
        "credentials_provider": conf.get("adfs-idp", "credentials_provider"),
        "region": conf.get("adfs-idp", "region"),
        "cluster_identifier": conf.get("adfs-idp", "cluster_identifier"),
        "idp_host": conf.get("adfs-idp", "idp_host"),
    }
    return db_connect


@pytest.fixture(scope="class")
def jwt_google_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "iam": conf.getboolean("jwt-google-idp", "iam"),
        "user": conf.get("database", "user"),
        "password": conf.get("jwt-google-idp", "password"),
        "credentials_provider": conf.get("jwt-google-idp", "credentials_provider"),
        "region": conf.get("jwt-google-idp", "region"),
        "cluster_identifier": conf.get("jwt-google-idp", "cluster_identifier"),
        "web_identity_token": conf.get("jwt-google-idp", "web_identity_token"),
        "preferred_role": conf.get("jwt-google-idp", "preferred_role"),
    }
    return db_connect


@pytest.fixture(scope="class")
def jwt_azure_v2_idp():
    db_connect = {
        "database": conf.get("database", "database"),
        "host": conf.get("database", "host"),
        "port": conf.getint("database", "port"),
        "db_user": conf.get("database", "user"),
        "ssl": conf.getboolean("database", "ssl"),
        "sslmode": conf.get("database", "sslmode"),
        "iam": conf.getboolean("jwt-azure-v2-idp", "iam"),
        "user": conf.get("database", "user"),
        "password": conf.get("jwt-azure-v2-idp", "password"),
        "credentials_provider": conf.get("jwt-azure-v2-idp", "credentials_provider"),
        "region": conf.get("jwt-azure-v2-idp", "region"),
        "cluster_identifier": conf.get("jwt-azure-v2-idp", "cluster_identifier"),
        "web_identity_token": conf.get("jwt-azure-v2-idp", "web_identity_token"),
        "preferred_role": conf.get("jwt-azure-v2-idp", "preferred_role"),
    }
    return db_connect


@pytest.fixture
def con(request, db_kwargs):
    conn = redshift_connector.connect(**db_kwargs)

    def fin():
        conn.rollback()
        try:
            conn.close()
        except redshift_connector.InterfaceError:
            pass

    request.addfinalizer(fin)
    return conn


@pytest.fixture
def cursor(request, con):
    cursor = con.cursor()

    def fin():
        cursor.close()

    request.addfinalizer(fin)
    return cursor


@pytest.fixture
def idp_arg(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def is_java():
    return "java" in sys.platform.lower()
