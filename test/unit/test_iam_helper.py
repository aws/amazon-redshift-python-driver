import typing
from test.unit import MockCredentialsProvider

import pytest  # type: ignore
from pytest_mock import mocker

from redshift_connector import InterfaceError, RedshiftProperty, set_iam_properties
from redshift_connector.auth import AWSCredentialsProvider
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


def get_set_iam_properties_args(**kwargs) -> typing.Dict[str, typing.Any]:
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
        "database_metadata_current_db_only": True,
        "access_key_id": None,
        "secret_access_key": None,
        "session_token": None,
        "profile": None,
        "ssl_insecure": None,
        **kwargs,
    }


required_params: typing.List[str] = ["info", "user", "host", "database", "port", "password"]


@pytest.mark.usefixtures("mock_set_iam_credentials")
@pytest.mark.parametrize("missing_param", required_params)
def test_set_iam_properties_fails_when_info_is_none(missing_param):
    keywords: typing.Dict = {missing_param: None}
    with pytest.raises(InterfaceError) as excinfo:
        set_iam_properties(**get_set_iam_properties_args(**keywords))
    assert "Invalid connection property setting" in str(excinfo.value)


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
def test_set_iam_properties_enforce_client_protocol_version(_input):
    keywords: typing.Dict = {"client_protocol_version": _input}
    all_params: typing.Dict = get_set_iam_properties_args(**keywords)
    assert all_params["client_protocol_version"] == _input

    set_iam_properties(**all_params)
    assert all_params["info"].client_protocol_version == _input


multi_req_params: typing.List[typing.Tuple[typing.Dict, str]] = [
    ({"ssl": False, "iam": True}, "Invalid connection property setting. SSL must be enabled when using IAM"),
    (
        {"iam": False, "credentials_provider": "anything"},
        "Invalid connection property setting",
    ),
    (
        {"iam": False, "profile": "default"},
        "Invalid connection property setting",
    ),
    (
        {"iam": False, "access_key_id": "my_key"},
        "Invalid connection property setting",
    ),
    (
        {"iam": False, "secret_access_key": "shh it's a secret"},
        "Invalid connection property setting",
    ),
    (
        {"iam": False, "session_token": "my_session"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "access_key_id": "my_key", "credentials_provider": "OktaCredentialsProvider"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "secret_access_key": "my_secret", "credentials_provider": "OktaCredentialsProvider"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "session_token": "token", "credentials_provider": "OktaCredentialsProvider"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "profile": "default", "credentials_provider": "OktaCredentialsProvider"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "profile": "default", "access_key_id": "my_key"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "profile": "default", "secret_access_key": "my_secret"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "profile": "default", "session_token": "token"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "secret_access_key": "my_secret"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "session_token": "token"},
        "Invalid connection property setting",
    ),
    (
        {"iam": True, "ssl": True, "access_key_id": "my_key", "password": ""},
        "Invalid connection property setting",
    ),
    (
        {"iam": False, "ssl_insecure": False},
        "Invalid connection property setting",
    ),
    (
        {"iam": False, "ssl_insecure": True},
        "Invalid connection property setting",
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


valid_aws_credential_args: typing.List[typing.Dict[str, str]] = [
    {"profile": "default"},
    {"access_key_id": "myAccessKey", "secret_access_key": "mySecret"},
    {"access_key_id": "myAccessKey", "password": "myHiddenSecret"},
    {"access_key_id": "myAccessKey", "secret_access_key": "mySecret", "session_token": "mySession"},
]


@pytest.mark.parametrize("test_input", valid_aws_credential_args)
def test_set_iam_properties_via_aws_credentials(mocker, test_input):
    # spy = mocker.spy("redshift_connector", "set_iam_credentials")
    info_obj: typing.Dict[str, typing.Any] = get_set_iam_properties_args(**test_input)
    info_obj["ssl"] = True
    info_obj["iam"] = True
    info_obj["cluster_identifier"] = "blah"

    mocker.patch("redshift_connector.iam_helper.set_iam_credentials", return_value=None)
    set_iam_properties(**info_obj)

    for aws_cred_key, aws_cred_val in enumerate(test_input):
        if aws_cred_key == "profile":
            assert info_obj["info"].profile == aws_cred_val
        if aws_cred_key == "access_key_id":
            assert info_obj["info"].access_key_id == aws_cred_val
        if aws_cred_key == "secret_access_key":
            assert info_obj["info"].secret_access_key == aws_cred_val
        if aws_cred_key == "password":
            assert info_obj["info"].password == aws_cred_val
        if aws_cred_key == "session_token":
            assert info_obj["info"].session_token == aws_cred_val


def test_set_iam_credentials_via_aws_credentials(mocker):
    redshift_property: RedshiftProperty = RedshiftProperty()
    redshift_property.profile = "profile_val"
    redshift_property.access_key_id = "access_val"
    redshift_property.secret_access_key = "secret_val"
    redshift_property.session_token = "session_val"

    mocker.patch("redshift_connector.iam_helper.set_cluster_credentials", return_value=None)
    spy = mocker.spy(AWSCredentialsProvider, "add_parameter")

    set_iam_credentials(redshift_property)
    assert spy.called is True
    assert spy.call_count == 1
    assert spy.call_args[0][1] == redshift_property


def test_dynamically_loading_credential_holder(mocker):
    external_class_name: str = "test.unit.MockCredentialsProvider"
    mocker.patch("{}.get_credentials".format(external_class_name))
    mocker.patch("redshift_connector.iam_helper.set_cluster_credentials", return_value=None)
    rp: RedshiftProperty = make_redshift_property()
    rp.credentials_provider = external_class_name

    spy = mocker.spy(MockCredentialsProvider, "add_parameter")

    set_iam_credentials(rp)
    assert spy.called
    assert spy.call_count == 1
    # ensure call to add_Parameter was made on the expected Provider class
    assert isinstance(spy.call_args[0][0], MockCredentialsProvider) is True
