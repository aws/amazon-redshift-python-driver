import typing

import pytest  # type: ignore
from pytest_mock import mocker

from redshift_connector import InterfaceError, RedshiftProperty, set_iam_properties
from redshift_connector.config import ClientProtocolVersion
from redshift_connector.iam_helper import set_iam_credentials
from redshift_connector.plugin import (
    AdfsCredentialsProvider,
    AzureCredentialsProvider,
    BrowserAzureCredentialsProvider,
    BrowserSamlCredentialsProvider,
    OktaCredentialsProvider,
    PingCredentialsProvider,
)


@pytest.fixture
def mock_set_iam_credentials(mocker):
    mocker.patch("redshift_connector.iam_helper.set_iam_credentials", return_value=None)


@pytest.fixture
def mock_set_cluster_credentials(mocker):
    mocker.patch("redshift_connector.iam_helper.set_cluster_credentials", return_value=None)


@pytest.fixture
def mock_all_provider_get_credentials(mocker):
    for provider in [
        "OktaCredentialsProvider",
        "AzureCredentialsProvider",
        "BrowserAzureCredentialsProvider",
        "PingCredentialsProvider",
        "AdfsCredentialsProvider",
        "BrowserSamlCredentialsProvider",
        "SamlCredentialsProvider",
    ]:
        mocker.patch("redshift_connector.plugin.{}.get_credentials".format(provider), return_value=None)


def get_set_iam_properties_args(**kwargs):
    return {
        "info": RedshiftProperty(),
        "user": "awsuser",
        "host": "localhost",
        "database": "dev",
        "port": 5439,
        "password": "hunter2",
        "source_address": None,
        "unix_sock": None,
        "ssl": False,
        "sslmode": "verify-ca",
        "timeout": None,
        "max_prepared_statements": 1,
        "tcp_keepalive": True,
        "application_name": None,
        "replication": None,
        "idp_host": None,
        "db_user": None,
        "iam": False,
        "app_id": None,
        "app_name": "testing",
        "preferred_role": None,
        "principal_arn": None,
        "credentials_provider": None,
        "region": None,
        "cluster_identifier": None,
        "client_id": None,
        "idp_tenant": None,
        "client_secret": None,
        "partner_sp_id": None,
        "idp_response_timeout": 1,
        "listen_port": 8000,
        "login_url": None,
        "auto_create": True,
        "db_groups": None,
        "force_lowercase": True,
        "allow_db_user_override": True,
        "client_protocol_version": ClientProtocolVersion.BASE_SERVER,
        **kwargs,
    }


required_params: typing.List[str] = ["info", "user", "host", "database", "port", "password"]


@pytest.mark.usefixtures("mock_set_iam_credentials")
@pytest.mark.parametrize("missing_param", required_params)
def test_set_iam_properties_fails_when_info_is_none(missing_param):
    keywords: typing.Dict = {missing_param: None}
    with pytest.raises(InterfaceError) as excinfo:
        set_iam_properties(**get_set_iam_properties_args(**keywords))
    assert "Invalid connection property setting. {} must be specified".format(missing_param) in str(excinfo.value)


ssl_mode_descriptions: typing.List[typing.Tuple[typing.Optional[str], str]] = [
    ("verify-ca", "verify-ca"),
    ("verify-full", "verify-full"),
    ("disable", "verify-ca"),
    ("allow", "verify-ca"),
    ("prefer", "verify-ca"),
    ("require", "verify-ca"),
    ("bogus", "verify-ca"),
    (None, "verify-ca"),
]


@pytest.mark.usefixtures("mock_set_iam_credentials")
@pytest.mark.parametrize("ssl_param", ssl_mode_descriptions)
def test_set_iam_properties_enforce_min_ssl_mode(ssl_param):
    test_input, expected_mode = ssl_param
    keywords: typing.Dict = {"sslmode": test_input, "ssl": True}
    all_params: typing.Dict = get_set_iam_properties_args(**keywords)
    assert all_params["sslmode"] == test_input

    set_iam_properties(**all_params)
    assert all_params["info"].sslmode == expected_mode


client_protocol_version_values: typing.List[int] = ClientProtocolVersion.list()


@pytest.mark.parametrize("_input", client_protocol_version_values)
def test_set_iam_properties_enforce_min_ssl_mode(_input):
    keywords: typing.Dict = {"client_protocol_version": _input}
    all_params: typing.Dict = get_set_iam_properties_args(**keywords)
    assert all_params["client_protocol_version"] == _input

    set_iam_properties(**all_params)
    assert all_params["info"].client_protocol_version == _input


multi_req_params: typing.List[typing.Tuple[typing.Dict, str]] = [
    ({"ssl": False, "iam": True}, "Invalid connection property setting. SSL must be enabled when using IAM"),
    (
        {"iam": False, "credentials_provider": "anything"},
        "Invalid connection property setting. IAM must be enabled when using credentials via identity provider",
    ),
    (
        {"iam": True, "ssl": True},
        "Invalid connection property setting. Credentials provider cannot be None when IAM is enabled",
    ),
]


@pytest.mark.usefixtures("mock_set_iam_credentials")
@pytest.mark.parametrize("joint_params", multi_req_params)
def test_set_iam_properties_enforce_setting_compatibility(mocker, joint_params):
    test_input, expected_exception_msg = joint_params

    with pytest.raises(InterfaceError) as excinfo:
        set_iam_properties(**get_set_iam_properties_args(**test_input))
    assert expected_exception_msg in str(excinfo.value)


valid_credential_providers: typing.List[typing.Tuple[str, typing.Any]] = [
    ("OktaCredentialsProvider", OktaCredentialsProvider),
    ("AzureCredentialsProvider", AzureCredentialsProvider),
    ("BrowserAzureCredentialsProvider", BrowserAzureCredentialsProvider),
    ("PingCredentialsProvider", PingCredentialsProvider),
    ("BrowserSamlCredentialsProvider", BrowserSamlCredentialsProvider),
    ("AdfsCredentialsProvider", AdfsCredentialsProvider),
]


def make_redshift_property() -> RedshiftProperty:
    rp: RedshiftProperty = RedshiftProperty()
    rp.user_name = "mario@luigi.com"
    rp.password = "bowser"
    rp.idp_host = "8000"
    rp.duration = 100
    rp.preferred_role = "analyst"
    rp.sslInsecure = False
    rp.db_user = "primary"
    rp.db_groups = ["employees"]
    rp.force_lowercase = True
    rp.auto_create = False
    rp.region = "us-west-1"
    rp.principal = "arn:aws:iam::123456789012:user/Development/product_1234/*"
    rp.client_protocol_version = ClientProtocolVersion.BASE_SERVER
    return rp


def mock_add_parameter():
    return mocker.patch


@pytest.mark.usefixtures("mock_all_provider_get_credentials")
@pytest.mark.usefixtures("mock_set_cluster_credentials")
@pytest.mark.parametrize("provider", valid_credential_providers)
def test_set_iam_properties_provider_assigned(mocker, provider):
    test_input, expectedProvider = provider
    mocker.patch("redshift_connector.plugin.{}.get_credentials".format(test_input))
    rp: RedshiftProperty = make_redshift_property()
    rp.credentials_provider = test_input

    spy = mocker.spy(expectedProvider, "add_parameter")

    set_iam_credentials(rp)
    assert spy.called
    assert spy.call_count == 1
    # ensure call to add_Parameter was made on the expected Provider class
    assert isinstance(spy.call_args[0][0], expectedProvider) is True
