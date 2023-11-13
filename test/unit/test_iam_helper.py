import datetime
import typing
from test.unit import MockCredentialsProvider
from unittest import mock
from unittest.mock import MagicMock, call

import pytest  # type: ignore
from dateutil.tz import tzutc
from pytest_mock import mocker  # type: ignore

from redshift_connector import InterfaceError, ProgrammingError, RedshiftProperty
from redshift_connector.auth import AWSCredentialsProvider
from redshift_connector.config import ClientProtocolVersion
from redshift_connector.credentials_holder import CredentialsHolder
from redshift_connector.iam_helper import IamHelper
from redshift_connector.idp_auth_helper import IdpAuthHelper
from redshift_connector.plugin import (
    AdfsCredentialsProvider,
    AzureCredentialsProvider,
    BasicJwtCredentialsProvider,
    BrowserAzureCredentialsProvider,
    BrowserSamlCredentialsProvider,
    OktaCredentialsProvider,
    PingCredentialsProvider,
)

from .helpers import make_redshift_property


@pytest.fixture
def mock_set_iam_credentials(mocker):
    mocker.patch("redshift_connector.iam_helper.IamHelper.set_iam_credentials", return_value=None)


@pytest.fixture
def mock_set_cluster_credentials(mocker):
    mocker.patch("redshift_connector.iam_helper.IamHelper.set_cluster_credentials", return_value=None)


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


def make_basic_redshift_property(**kwargs) -> RedshiftProperty:
    rp: RedshiftProperty = RedshiftProperty()
    rp.put("user_name", "awsuser")
    rp.put("host", "redshift-cluster-1.abcdefghijk.us-east-1.redshift.amazonaws.com")
    rp.put("db_name", "dev")
    for k, v in kwargs.items():
        rp.put(k, v)
    return rp


@pytest.mark.usefixtures("mock_set_iam_credentials")
def test_set_iam_properties_fails_when_non_str_credential_provider() -> None:
    keywords: typing.Dict[str, typing.Union[int, bool]] = {
        "credentials_provider": 1,
        "iam": True,
    }
    with pytest.raises(InterfaceError) as excinfo:
        IamHelper.set_iam_properties(make_basic_redshift_property(**keywords))
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
def test_set_iam_properties_enforce_min_ssl_mode(ssl_param) -> None:
    test_input, expected_mode = ssl_param
    keywords: typing.Dict[str, typing.Union[str, bool]] = {
        "sslmode": test_input,
        "ssl": True,
        "host": "redshift-cluster-1.aaaaaaaaaaaa.us-east-1.redshift.amazonaws.com",
    }
    rp: RedshiftProperty = make_basic_redshift_property(**keywords)
    if test_input is None:
        assert rp.sslmode == expected_mode
    else:
        assert rp.sslmode == test_input

    IamHelper.set_iam_properties(rp)
    assert rp.sslmode == expected_mode


client_protocol_version_values: typing.List[int] = ClientProtocolVersion.list()


@pytest.mark.parametrize("_input", client_protocol_version_values)
def test_set_iam_properties_enforce_client_protocol_version(_input) -> None:
    keywords: typing.Dict[str, typing.Union[str, int]] = {
        "client_protocol_version": _input,
        "host": "redshift-cluster-1.aaaaaaaaaaaa.us-east-1.redshift.amazonaws.com",
    }
    rp: RedshiftProperty = make_basic_redshift_property(**keywords)
    assert rp.client_protocol_version == _input

    IamHelper.set_iam_properties(rp)
    assert rp.client_protocol_version == _input


multi_req_params: typing.List[typing.Tuple[typing.Dict, str]] = [
    (
        {"ssl": False, "iam": True},
        "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled",
    ),
    (
        {"iam": True, "ssl": True},
        "Invalid connection property setting",
    ),
    (
        {
            "iam": True,
            "ssl": True,
            "access_key_id": "my_key",
            "credentials_provider": "OktaCredentialsProvider",
            "cluster_identifier": "my_cluster",
        },
        "Invalid connection property setting. It is not valid to provide both Credentials provider and AWS credentials or AWS profile",
    ),
    (
        {
            "iam": True,
            "ssl": True,
            "secret_access_key": "my_secret",
            "credentials_provider": "OktaCredentialsProvider",
            "cluster_identifier": "my_cluster",
        },
        "Invalid connection property setting. It is not valid to provide both Credentials provider and AWS credentials or AWS profile",
    ),
    (
        {
            "iam": True,
            "ssl": True,
            "session_token": "token",
            "credentials_provider": "OktaCredentialsProvider",
            "cluster_identifier": "my_cluster",
        },
        "Invalid connection property setting. It is not valid to provide both Credentials provider and AWS credentials or AWS profile",
    ),
    (
        {
            "iam": True,
            "ssl": True,
            "profile": "default",
            "credentials_provider": "OktaCredentialsProvider",
            "cluster_identifier": "my_cluster",
        },
        "Invalid connection property setting. It is not valid to provide both Credentials provider and AWS credentials or AWS profile",
    ),
    (
        {"iam": True, "ssl": True, "credentials_provider": 1, "cluster_identifier": "my_cluster"},
        "Invalid connection property setting. It is not valid to provide a non-string value to credentials_provider.",
    ),
    (
        {"iam": True, "ssl": True, "profile": "default", "access_key_id": "my_key", "cluster_identifier": "my_cluster"},
        "Invalid connection property setting. It is not valid to provide any of access_key_id, secret_access_key, or session_token when profile is provided",
    ),
    (
        {
            "iam": True,
            "ssl": True,
            "profile": "default",
            "secret_access_key": "my_secret",
            "cluster_identifier": "my_cluster",
        },
        "Invalid connection property setting. It is not valid to provide any of access_key_id, secret_access_key, or session_token when profile is provided",
    ),
    (
        {"iam": True, "ssl": True, "profile": "default", "session_token": "token", "cluster_identifier": "my_cluster"},
        "Invalid connection property setting. It is not valid to provide any of access_key_id, secret_access_key, or session_token when profile is provided",
    ),
    (
        {"iam": True, "ssl": True, "access_key_id": "my_access_key", "cluster_identifier": "my_cluster"},
        "Invalid connection property setting. secret access key must be provided in either secret_access_key or password field",
    ),
    (
        {"iam": True, "ssl": True, "cluster_identifier": "my_cluster", "secret_access_key": "someSecretAccessKey"},
        "Invalid connection property setting. access_key_id is required when secret_access_key is provided",
    ),
    (
        {"iam": True, "ssl": True, "cluster_identifier": "my_cluster", "session_token": "someSessionToken"},
        "Invalid connection property setting. access_key_id and secret_access_key are  required when session_token is provided",
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
        {
            "iam": True,
            "ssl": True,
        },
        "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled",
    ),
    (
        {"iam": True, "ssl": True, "credentials_provider": "SomeProvider"},
        "Invalid connection property setting. cluster_identifier must be provided when IAM is enabled",
    ),
    (
        {"iam": True, "ssl": True, "cluster_identifier": "some-cluster", "auth_profile": "someAuthProfile"},
        "Invalid connection property setting. access_key_id, secret_access_key, and region are required for authentication via Redshift auth_profile",
    ),
]


@pytest.mark.usefixtures("mock_set_iam_credentials")
@pytest.mark.parametrize("joint_params", multi_req_params)
def test_set_iam_properties_enforce_setting_compatibility(mocker, joint_params) -> None:
    test_input, expected_exception_msg = joint_params

    with pytest.raises(InterfaceError) as excinfo:
        IamHelper.set_iam_properties(make_basic_redshift_property(**test_input))
    assert expected_exception_msg in str(excinfo.value)


valid_credential_providers: typing.List[typing.Tuple[str, typing.Any]] = [
    ("OktaCredentialsProvider", OktaCredentialsProvider),
    ("AzureCredentialsProvider", AzureCredentialsProvider),
    ("BrowserAzureCredentialsProvider", BrowserAzureCredentialsProvider),
    ("PingCredentialsProvider", PingCredentialsProvider),
    ("BrowserSamlCredentialsProvider", BrowserSamlCredentialsProvider),
    ("AdfsCredentialsProvider", AdfsCredentialsProvider),
    ("BasicJwtCredentialsProvider", BasicJwtCredentialsProvider),
]


def test_set_iam_properties_raises_exception_when_info_is_none() -> None:
    with pytest.raises(InterfaceError) as excinfo:
        IamHelper.set_auth_properties(None)  # type: ignore
    assert "Invalid connection property setting. info must be specified" in str(excinfo.value)


def mock_add_parameter():
    return mocker.patch


@pytest.mark.usefixtures("mock_all_provider_get_credentials")
@pytest.mark.usefixtures("mock_set_cluster_credentials")
@pytest.mark.parametrize("provider", valid_credential_providers)
def test_set_iam_properties_provider_assigned(mocker, provider) -> None:
    test_input, expectedProvider = provider
    mocker.patch("redshift_connector.plugin.{}.get_credentials".format(test_input))
    rp: RedshiftProperty = make_redshift_property()
    rp.credentials_provider = test_input

    spy = mocker.spy(expectedProvider, "add_parameter")

    IamHelper.set_iam_credentials(rp)
    assert spy.called
    assert spy.call_count == 1
    # ensure call to add_Parameter was made on the expected Provider class
    assert isinstance(spy.call_args[0][0], expectedProvider) is True


valid_aws_credential_args: typing.List[typing.Dict[str, str]] = [
    {"user": "", "password": "", "profile": "default"},
    {"user": "", "password": "", "access_key_id": "myAccessKey", "secret_access_key": "mySecret"},
    {"user": "", "secret_access_key": "", "access_key_id": "myAccessKey", "password": "myHiddenSecret"},
    {
        "user": "",
        "password": "",
        "access_key_id": "myAccessKey",
        "secret_access_key": "mySecret",
        "session_token": "mySession",
    },
    {"user": "", "password": "", "credentials_provider": "BrowserSamlCredentialsProvider"},
]


@pytest.mark.parametrize("test_input", valid_aws_credential_args)
def test_set_iam_properties_via_aws_credentials(mocker, test_input) -> None:
    # spy = mocker.spy("redshift_connector", "set_iam_credentials")
    rp: RedshiftProperty = make_basic_redshift_property(
        **{**test_input, **{"ssl": True, "iam": True, "cluster_identifier": "blah"}}
    )

    mocker.patch("redshift_connector.iam_helper.IamHelper.set_iam_credentials", return_value=None)
    IamHelper.set_iam_properties(rp)

    for aws_cred_key, aws_cred_val in enumerate(test_input):
        if aws_cred_key == "profile":
            assert rp.profile == aws_cred_val
        if aws_cred_key == "access_key_id":
            assert rp.access_key_id == aws_cred_val
        if aws_cred_key == "secret_access_key":
            assert rp.secret_access_key == aws_cred_val
        if aws_cred_key == "password":
            assert rp.password == aws_cred_val
        if aws_cred_key == "session_token":
            assert rp.session_token == aws_cred_val


def test_set_iam_credentials_via_aws_credentials(mocker) -> None:
    redshift_property: RedshiftProperty = RedshiftProperty()
    redshift_property.profile = "profile_val"
    redshift_property.access_key_id = "access_val"
    redshift_property.secret_access_key = "secret_val"
    redshift_property.session_token = "session_val"
    redshift_property.host = "redshift-cluster-1.aaaaaaaaaaaa.amazonaws.com"

    mocker.patch("redshift_connector.iam_helper.IamHelper.set_cluster_credentials", return_value=None)
    spy = mocker.spy(AWSCredentialsProvider, "add_parameter")

    IamHelper.set_iam_credentials(redshift_property)
    assert spy.called is True
    assert spy.call_count == 1
    assert spy.call_args[0][1] == redshift_property


@mock.patch("boto3.client")
def test_set_iam_credentials_for_serverless_uses_redshift_serverless_client(
    mock_boto_client, serverless_iam_db_kwargs
) -> None:
    rp: RedshiftProperty = RedshiftProperty()

    for k, v in serverless_iam_db_kwargs.items():
        rp.put(k, v)

    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_holder.has_associated_session = False

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)

    # ensure client creation
    assert mock_boto_client.called is True
    assert mock_boto_client.call_count == 1
    assert mock_boto_client.call_args[1]["service_name"] == "redshift-serverless"


@mock.patch("boto3.client")
@pytest.mark.parametrize(
    "serverless_host",
    (
        "testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com",
        "012345678901.us-east-2.redshift-serverless.amazonaws.com",
    ),
)
def test_set_iam_credentials_for_serverless_calls_get_credentials(
    mock_boto_client, serverless_host, mocker, serverless_iam_db_kwargs
) -> None:
    rp: RedshiftProperty = RedshiftProperty()

    for k, v in serverless_iam_db_kwargs.items():
        rp.put(k, v)

    rp.set_serverless_acct_id()
    rp.set_region_from_host()
    rp.set_serverless_work_group_from_host()

    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_holder.has_associated_session = False

    spy = mocker.spy(rp, "put")

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)

    # ensure describe_configuration is called
    if rp.serverless_work_group:
        mock_boto_client.assert_has_calls(
            [call().get_credentials(dbName=rp.db_name, workgroupName=rp.serverless_work_group)]
        )
    else:
        mock_boto_client.assert_has_calls([call().get_credentials(dbName=rp.db_name)])

    # ensure host, port values were set
    assert spy.called

    # ensure RedshiftProperty.put method was called
    assert "user_name" in [c[0][0] for c in spy.call_args_list]
    assert "password" in [c[0][0] for c in spy.call_args_list]


def test_serverless_properties_used_when_is_serverless_true(mocker) -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.host = "test-endpoint-xxxx.123456789123.us-east-2.redshift-serverless.amazonaws.com"
    rp.is_serverless = True
    rp.serverless_work_group = "something"
    rp.serverless_acct_id = "111111111111"

    mocker.patch(
        "redshift_connector.native_plugin_helper.NativeAuthPluginHelper.get_native_auth_plugin_credentials",
        return_value=None,
    )

    result = IamHelper.set_iam_properties(rp)

    assert result.is_serverless == True
    assert result.serverless_work_group == rp.serverless_work_group
    assert result.serverless_acct_id == rp.serverless_acct_id


def test_internal_is_serverless_prop_true_for_nlb_host() -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.is_serverless = True

    assert rp._is_serverless is True


@pytest.mark.parametrize(
    "serverless_host",
    (
        "testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com",
        "012345678901.us-east-2.redshift-serverless.amazonaws.com",
    ),
)
def test_internal_is_serverless_prop_true_for_serverless_host(serverless_host) -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.host = serverless_host
    rp.serverless_work_group = "something"
    rp.serverless_acct_id = "111111111111"

    assert rp._is_serverless is True


def test_dynamically_loading_credential_holder(mocker) -> None:
    external_class_name: str = "test.unit.MockCredentialsProvider"
    mocker.patch("{}.get_credentials".format(external_class_name))
    mocker.patch("redshift_connector.iam_helper.IamHelper.set_cluster_credentials", return_value=None)
    rp: RedshiftProperty = make_redshift_property()
    rp.credentials_provider = external_class_name

    spy = mocker.spy(MockCredentialsProvider, "add_parameter")

    IamHelper.set_iam_credentials(rp)
    assert spy.called
    assert spy.call_count == 1
    # ensure call to add_Parameter was made on the expected Provider class
    assert isinstance(spy.call_args[0][0], MockCredentialsProvider) is True


def test_get_credentials_cache_key() -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.db_user = "2"
    rp.db_name = "1"
    rp.db_groups = ["4", "3", "5"]
    rp.cluster_identifier = "6"
    rp.auto_create = False

    res_cache_key: str = IamHelper.get_credentials_cache_key(rp, None)  # type: ignore
    assert res_cache_key is not None
    assert res_cache_key == "2;1;3,4,5;6;False;900"


def test_get_credentials_cache_key_no_db_groups() -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.db_user = "2"
    rp.db_name = "1"
    rp.cluster_identifier = "6"
    rp.auto_create = False

    res_cache_key: str = IamHelper.get_credentials_cache_key(rp, None)  # type: ignore
    assert res_cache_key is not None
    assert res_cache_key == "2;1;6;False;900"


@mock.patch("boto3.client.get_cluster_credentials")
@mock.patch("boto3.client")
def test_set_cluster_credentials_uses_custom_domain_name_if_custom_domain_name_cluster(
    mock_boto_client, mock_get_cluster_credentials
) -> None:
    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_provider.get_cache_key.return_value = "mocked"
    mock_cred_holder.has_associated_session = False

    rp: RedshiftProperty = make_redshift_property()
    rp.put("cluster_identifier", None)
    rp.put("host", "mycustom.domain.name")
    rp.put("is_cname", True)

    IamHelper.credentials_cache.clear()

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)

    assert mock_boto_client.called is True
    mock_boto_client.assert_has_calls(
        [
            call().get_cluster_credentials(
                AutoCreate=rp.auto_create,
                CustomDomainName=rp.host,
                DbGroups=rp.db_groups,
                DbName=rp.db_name,
                DbUser=rp.db_user,
            )
        ]
    )


@mock.patch("boto3.client.get_cluster_credentials")
@mock.patch("boto3.client")
def test_set_cluster_credentials_caches_credentials(mock_boto_client, mock_get_cluster_credentials) -> None:
    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_provider.get_cache_key.return_value = "mocked"
    mock_cred_holder.has_associated_session = False

    rp: RedshiftProperty = make_redshift_property()

    IamHelper.credentials_cache.clear()

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)
    assert len(IamHelper.credentials_cache) == 1

    assert mock_boto_client.called is True
    mock_boto_client.assert_has_calls(
        [
            call().get_cluster_credentials(
                AutoCreate=rp.auto_create,
                ClusterIdentifier=rp.cluster_identifier,
                DbGroups=rp.db_groups,
                DbName=rp.db_name,
                DbUser=rp.db_user,
            )
        ]
    )


@mock.patch("boto3.client.get_cluster_credentials")
@mock.patch("boto3.client")
def test_set_cluster_credentials_honors_iam_disable_cache(mock_boto_client, mock_get_cluster_credentials) -> None:
    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_holder.has_associated_session = False

    rp: RedshiftProperty = make_redshift_property()
    rp.iam_disable_cache = True

    IamHelper.credentials_cache.clear()

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)
    assert len(IamHelper.credentials_cache) == 0

    assert mock_boto_client.called is True
    mock_boto_client.assert_has_calls(
        [
            call().get_cluster_credentials(
                AutoCreate=rp.auto_create,
                ClusterIdentifier=rp.cluster_identifier,
                DbGroups=rp.db_groups,
                DbName=rp.db_name,
                DbUser=rp.db_user,
            )
        ]
    )


@mock.patch("boto3.client.get_cluster_credentials")
@mock.patch("boto3.client")
def test_set_cluster_credentials_ignores_cache_when_disabled(mock_boto_client, mock_get_cluster_credentials) -> None:
    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_holder.has_associated_session = False

    rp: RedshiftProperty = make_redshift_property()
    rp.iam_disable_cache = True
    # mock out the boto3 response temporary credentials stored from prior auth
    mock_cred_obj: typing.Dict[str, typing.Union[str, datetime.datetime]] = {
        "DbUser": "xyz",
        "DbPassword": "turtle",
        "Expiration": datetime.datetime(9999, 1, 1, tzinfo=tzutc()),
    }
    # populate the cache
    IamHelper.credentials_cache.clear()
    IamHelper.credentials_cache[IamHelper.get_credentials_cache_key(rp, mock_cred_provider)] = mock_cred_obj

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)
    assert len(IamHelper.credentials_cache) == 1
    assert IamHelper.credentials_cache[IamHelper.get_credentials_cache_key(rp, mock_cred_provider)] is mock_cred_obj
    assert mock_boto_client.called is True

    # we should not have retrieved user/password from the cache
    assert rp.user_name != mock_cred_obj["DbUser"]
    assert rp.password != mock_cred_obj["DbPassword"]

    assert (
        call().get_cluster_credentials(
            AutoCreate=rp.auto_create,
            ClusterIdentifier=rp.cluster_identifier,
            DbGroups=rp.db_groups,
            DbName=rp.db_name,
            DbUser=rp.db_user,
        )
        in mock_boto_client.mock_calls
    )


@mock.patch("boto3.client.get_cluster_credentials")
@mock.patch("boto3.client")
def test_set_cluster_credentials_uses_cache_if_possible(mock_boto_client, mock_get_cluster_credentials) -> None:
    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_holder.has_associated_session = False

    rp: RedshiftProperty = make_redshift_property()
    # mock out the boto3 response temporary credentials stored from prior auth
    mock_cred_obj: typing.Dict[str, typing.Union[str, datetime.datetime]] = {
        "DbUser": "xyz",
        "DbPassword": "turtle",
        "Expiration": datetime.datetime(9999, 1, 1, tzinfo=tzutc()),
    }
    # populate the cache
    IamHelper.credentials_cache.clear()
    IamHelper.credentials_cache[IamHelper.get_credentials_cache_key(rp, mock_cred_provider)] = mock_cred_obj

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)
    assert len(IamHelper.credentials_cache) == 1
    assert IamHelper.credentials_cache[IamHelper.get_credentials_cache_key(rp, mock_cred_provider)] is mock_cred_obj
    assert mock_boto_client.called is True

    assert rp.user_name == mock_cred_obj["DbUser"]
    assert rp.password == mock_cred_obj["DbPassword"]

    assert (
        call().get_cluster_credentials(
            AutoCreate=rp.auto_create,
            ClusterIdentifier=rp.cluster_identifier,
            DbGroups=rp.db_groups,
            DbName=rp.db_name,
            DbUser=rp.db_user,
        )
        not in mock_boto_client.mock_calls
    )


@mock.patch("boto3.client.get_cluster_credentials")
@mock.patch("boto3.client")
def test_set_cluster_credentials_refreshes_stale_credentials(mock_boto_client, mock_get_cluster_credentials) -> None:
    mock_cred_provider = MagicMock()
    mock_cred_holder = MagicMock()
    mock_cred_provider.get_credentials.return_value = mock_cred_holder
    mock_cred_holder.has_associated_session = False

    rp: RedshiftProperty = make_redshift_property()
    # mock out the boto3 response temporary credentials stored from prior auth (now stale)
    mock_cred_obj: typing.Dict[str, typing.Union[str, datetime.datetime]] = {
        "DbUser": "xyz",
        "DbPassword": "turtle",
        "Expiration": datetime.datetime(1, 1, 1, tzinfo=tzutc()),
    }
    # populate the cache
    IamHelper.credentials_cache.clear()
    IamHelper.credentials_cache[IamHelper.get_credentials_cache_key(rp, mock_cred_provider)] = mock_cred_obj

    IamHelper.set_cluster_credentials(mock_cred_provider, rp)
    assert len(IamHelper.credentials_cache) == 1
    # ensure new temporary credentials have been replaced in cache
    assert IamHelper.get_credentials_cache_key(rp, mock_cred_provider) in IamHelper.credentials_cache
    assert IamHelper.credentials_cache[IamHelper.get_credentials_cache_key(rp, mock_cred_provider)] is not mock_cred_obj
    assert mock_boto_client.called is True

    mock_boto_client.assert_has_calls(
        [
            call().get_cluster_credentials(
                AutoCreate=rp.auto_create,
                ClusterIdentifier=rp.cluster_identifier,
                DbGroups=rp.db_groups,
                DbName=rp.db_name,
                DbUser=rp.db_user,
            )
        ]
    )


def test_get_authentication_type_for_iam_with_profile() -> None:
    provider = AWSCredentialsProvider()
    provider.profile = "test"
    assert IamHelper.get_authentication_type(provider) == IamHelper.IAMAuthenticationType.PROFILE


def test_get_authentication_type_for_iam_with_key_session() -> None:
    provider = AWSCredentialsProvider()
    provider.access_key_id = "test_key"
    provider.session_token = "test_token"
    provider.secret_access_key = "test_secret_key"
    assert IamHelper.get_authentication_type(provider) == IamHelper.IAMAuthenticationType.IAM_KEYS_WITH_SESSION


def test_get_authentication_type_for_iam_with_key() -> None:
    provider = AWSCredentialsProvider()
    provider.access_key_id = "test_key"
    provider.secret_access_key = "test_secret_key"
    assert IamHelper.get_authentication_type(provider) == IamHelper.IAMAuthenticationType.IAM_KEYS


def test_get_authentication_type_for_iam_with_plugin() -> None:
    provider = BrowserSamlCredentialsProvider()
    assert IamHelper.get_authentication_type(provider) == IamHelper.IAMAuthenticationType.PLUGIN


@pytest.mark.parametrize(
    "conn_params, provider, exp_result",
    (
        (
            {"credentials_provider": "BrowserSamlCredentialsProvider"},
            IamHelper.IAMAuthenticationType.PLUGIN,
            IamHelper.GetClusterCredentialsAPIType.IAM_V1,
        ),
        (
            {"group_federation": True},
            IamHelper.IAMAuthenticationType.PROFILE,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"is_serverless": True},
            IamHelper.IAMAuthenticationType.PROFILE,
            IamHelper.GetClusterCredentialsAPIType.SERVERLESS_V1,
        ),
        (
            {"is_serverless": True, "group_federation": True},
            IamHelper.IAMAuthenticationType.IAM_KEYS,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"group_federation": True},
            IamHelper.IAMAuthenticationType.IAM_KEYS,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"is_serverless": True},
            IamHelper.IAMAuthenticationType.IAM_KEYS,
            IamHelper.GetClusterCredentialsAPIType.SERVERLESS_V1,
        ),
        (
            {"is_serverless": True, "group_federation": True},
            IamHelper.IAMAuthenticationType.IAM_KEYS_WITH_SESSION,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"group_federation": True},
            IamHelper.IAMAuthenticationType.IAM_KEYS_WITH_SESSION,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"is_serverless": True},
            IamHelper.IAMAuthenticationType.IAM_KEYS_WITH_SESSION,
            IamHelper.GetClusterCredentialsAPIType.SERVERLESS_V1,
        ),
        (
            {"is_serverless": True, "group_federation": True},
            IamHelper.IAMAuthenticationType.PROFILE,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"group_federation": True, "credentials_provider": "BrowserSamlCredentialsProvider"},
            IamHelper.IAMAuthenticationType.PLUGIN,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"is_serverless": True, "group_federation": True, "credentials_provider": "BrowserSamlCredentialsProvider"},
            IamHelper.IAMAuthenticationType.PLUGIN,
            IamHelper.GetClusterCredentialsAPIType.IAM_V2,
        ),
        (
            {"is_serverless": True, "group_federation": True, "is_cname": True},
            IamHelper.IAMAuthenticationType.PROFILE,
            "Custom cluster names are not supported for Redshift Serverless",
        ),
    ),
)
def test_get_cluster_credentials_api_type_will_use_correct_api(conn_params, provider, exp_result) -> None:
    info = RedshiftProperty()
    for param in conn_params.items():
        info.put(param[0], param[1])

    if isinstance(exp_result, IamHelper.GetClusterCredentialsAPIType):
        assert IamHelper.get_cluster_credentials_api_type(info, provider) == exp_result
    else:
        with pytest.raises(InterfaceError, match=exp_result):
            IamHelper.get_cluster_credentials_api_type(info, provider)


@pytest.mark.parametrize(
    "boto3_version,connection_args",
    (
        ("1.17.110", {"iam": True, "ssl": True, "auth_profile": "SomeTestProfile", "cluster_identifier": "my_cluster"}),
        ("1.17.100", {"iam": True, "ssl": True, "auth_profile": "SomeTestProfile", "cluster_identifier": "my_cluster"}),
    ),
)
def test_set_iam_properties_raises_exception_when_insufficient_boto3_version(
    mocker, boto3_version, connection_args
) -> None:
    from packaging.version import Version

    mock_boto3_dist_obj = Version(boto3_version)

    mocker.patch("redshift_connector.idp_auth_helper.IdpAuthHelper.get_pkg_version", return_value=mock_boto3_dist_obj)
    rp = RedshiftProperty()
    for key, value in connection_args.items():
        rp.put(key, value)

    with pytest.raises(ModuleNotFoundError) as excinfo:
        IamHelper.set_iam_properties(rp)
    assert "boto3" in str(excinfo)
    assert "required for authentication" in str(excinfo.value)


def test_set_iam_properties_use_redshift_auth_profile_calls_read_auth_profile(mocker) -> None:
    mocker.patch(
        "redshift_connector.idp_auth_helper.IdpAuthHelper.read_auth_profile",
        return_value=RedshiftProperty(kwargs={"": ""}),
    )
    mocker.patch("redshift_connector.iam_helper.IamHelper.set_iam_credentials", return_value=None)
    spy = mocker.spy(IdpAuthHelper, "read_auth_profile")

    # anticipate read_auth_profile being called with the following parameters
    exp_call_arg: typing.Dict[str, str] = {
        "auth_profile": "someTestProfile",
        "access_key_id": "someAccessKeyIdValue",
        "session_token": "someSessionTokenValue",
        "secret_access_key": "someSecretAccessValue",
        "region": "someRegion",
        "endpoint_url": "someEndpointUrl",
    }

    rp: RedshiftProperty = make_basic_redshift_property(
        **{**{"iam": True, "ssl": True, "cluster_identifier": "someCluster"}, **exp_call_arg}
    )

    IamHelper.set_iam_properties(rp)

    assert spy.called is True
    assert spy.call_count == 1
    assert "auth_profile" in spy.call_args[1]
    assert spy.call_args[1]["auth_profile"] == exp_call_arg["auth_profile"]
    assert "iam_access_key_id" in spy.call_args[1]
    assert spy.call_args[1]["iam_access_key_id"] == exp_call_arg["access_key_id"]
    assert "iam_session_token" in spy.call_args[1]
    assert spy.call_args[1]["iam_session_token"] == exp_call_arg["session_token"]
    assert "iam_secret_key" in spy.call_args[1]
    assert spy.call_args[1]["iam_secret_key"] == exp_call_arg["secret_access_key"]
    assert "info" in spy.call_args[1]
    assert spy.call_args[1]["info"].region == exp_call_arg["region"]
    assert spy.call_args[1]["info"].endpoint_url == exp_call_arg["endpoint_url"]


def test_set_iam_properties_redshift_auth_profile_does_override(mocker) -> None:
    mock_contents: typing.Dict[str, str] = {
        "password": "overridePassword",
    }
    mock_auth_profile_contents: RedshiftProperty = RedshiftProperty(**mock_contents)
    mocker.patch(
        "redshift_connector.idp_auth_helper.IdpAuthHelper.read_auth_profile", return_value=mock_auth_profile_contents
    )
    mocker.patch("redshift_connector.iam_helper.IamHelper.set_iam_credentials", return_value=None)
    redshift_auth_profile_spy = mocker.spy(IdpAuthHelper, "read_auth_profile")
    set_iam_crednetials_spy = mocker.spy(IamHelper, "set_iam_credentials")

    exp_call_arg: typing.Dict[str, str] = {
        "auth_profile": "someTestProfile",
        "access_key_id": "someAccessKeyIdValue",
        "session_token": "someSessionTokenValue",
        "secret_access_key": "someSecretAccessValue",
        "region": "someRegion",
        "endpoint_url": "someEndpointUrl",
    }

    rp: RedshiftProperty = make_basic_redshift_property(
        **{
            **{"iam": True, "ssl": True, "cluster_identifier": "someCluster"},
            **exp_call_arg,
        }
    )

    res = IamHelper.set_iam_properties(rp)
    assert rp.password == mock_auth_profile_contents.password

    assert redshift_auth_profile_spy.called is True
    assert redshift_auth_profile_spy.call_count == 1
    assert res.password == mock_auth_profile_contents.password

    assert set_iam_crednetials_spy.called is True
    assert set_iam_crednetials_spy.call_count == 1
    assert set_iam_crednetials_spy.call_args[0][0].password == mock_auth_profile_contents.password


def test_read_auth_profile_raises_exception_if_profile_dne(mocker):
    from botocore import exceptions  # type: ignore

    req_params: typing.Dict = {
        "auth_profile": "testProfile",
        "iam_access_key_id": "testAccessKey",
        "iam_secret_key": "someSecretKey",
        "iam_session_token": "someToken",
        "info": RedshiftProperty(),
    }

    req_params["info"].put("region", "us-east-1")

    mock_redshift_client: MagicMock = MagicMock()
    mock_redshift_client.describe_authentication_profiles.side_effect = exceptions.ClientError(
        operation_name="ErrorOp", error_response=MagicMock()
    )
    mocker.patch("boto3.client", return_value=mock_redshift_client)

    with pytest.raises(InterfaceError):
        IamHelper.read_auth_profile(**req_params)


def test_read_auth_profile_loads_json_payload(mocker) -> None:
    import json

    req_params: typing.Dict = {
        "auth_profile": "testProfile",
        "iam_access_key_id": "testAccessKey",
        "iam_secret_key": "someSecretKey",
        "iam_session_token": "someToken",
        "info": RedshiftProperty(),
    }

    req_params["info"].put("region", "us-east-1")

    mock_payload: typing.Dict[str, str] = {"region": "someTestRegion", "cluster_identifier": "someCluster"}

    mock_redshift_client: MagicMock = MagicMock()
    mock_redshift_client.describe_authentication_profiles.return_value = {
        "AuthenticationProfiles": [{"AuthenticationProfileContent": json.dumps(mock_payload)}]
    }
    mocker.patch("boto3.client", return_value=mock_redshift_client)

    result = IamHelper.read_auth_profile(**req_params)
    assert result.region == mock_payload["region"]
    assert result.cluster_identifier == mock_payload["cluster_identifier"]


def test_read_auth_profile_invalid_json_payload_raises_exception(mocker) -> None:
    import json

    req_params: typing.Dict = {
        "auth_profile": "testProfile",
        "iam_access_key_id": "testAccessKey",
        "iam_secret_key": "someSecretKey",
        "iam_session_token": "someToken",
        "info": RedshiftProperty(),
    }

    req_params["info"].put("region", "us-east-1")

    mock_redshift_client: MagicMock = MagicMock()
    mock_redshift_client.describe_authentication_profiles.return_value = {
        "AuthenticationProfiles": [{"AuthenticationProfileContent": "{{{{"}]
    }
    mocker.patch("boto3.client", return_value=mock_redshift_client)

    with pytest.raises(
        ProgrammingError, match="Unable to decode the JSON content of the Redshift authentication profile"
    ):
        IamHelper.read_auth_profile(**req_params)


def test_set_iam_properties_calls_set_auth_props(mocker) -> None:
    mocker.patch("redshift_connector.iam_helper.IdpAuthHelper.set_auth_properties", return_value=None)
    spy = mocker.spy(IdpAuthHelper, "set_auth_properties")
    mock_rp: MagicMock = MagicMock()
    mock_rp.credentials_provider = None
    mock_rp._is_serverless = False
    mock_rp.is_cname = False
    IamHelper.set_iam_properties(mock_rp)

    assert spy.called is True
    assert spy.call_count == 1
    assert spy.call_args[0][0] == mock_rp


def test_set_cluster_identifier_calls_describe_custom_domain_associations(mocker) -> None:
    custom_domain_name: str = "my.custom.domain.com"
    rp: RedshiftProperty = RedshiftProperty()
    rp.put("host", custom_domain_name)

    mock_redshift_client: MagicMock = MagicMock()
    spy = mocker.spy(mock_redshift_client, "describe_custom_domain_associations")
    mocker.patch("redshift_connector.iam_helper.IamHelper.get_boto3_redshift_client", return_value=mock_redshift_client)

    IamHelper.set_cluster_identifier(MagicMock(), rp)

    assert spy.called is True
    assert spy.call_count == 1
    assert "CustomDomainName" in spy.call_args[1]
    assert spy.call_args[1]["CustomDomainName"] == custom_domain_name


def test_set_cluster_identifier_sets_cluster_identifier(mocker) -> None:
    exp_cluster_identifier: str = "my-cname-test"
    custom_domain_name: str = "my.custom.domain.com"
    rp: RedshiftProperty = RedshiftProperty()
    rp.put("host", custom_domain_name)
    mock_redshift_client: MagicMock = MagicMock()

    mocker.patch("redshift_connector.iam_helper.IamHelper.get_boto3_redshift_client", return_value=mock_redshift_client)
    mock_redshift_client.describe_custom_domain_associations.return_value = {
        "Associations": [
            {
                "CustomDomainCertificateArn": "arn:aws:acm:us-east-1:123456789123:certificate/5237b206-ffd3-11ed-be56-0242ac120002",
                "CustomDomainCertificateExpiryDate": datetime.datetime(2023, 10, 20, 23, 59, 59, tzinfo=tzutc()),
                "CertificateAssociations": [
                    {"CustomDomainName": custom_domain_name, "ClusterIdentifier": exp_cluster_identifier}
                ],
            }
        ],
        "ResponseMetadata": {
            "RequestId": "d432c0b8-12c5-43a4-b3b9-dfc66427bbd0",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amzn-requestid": "d432c0b8-12c5-43a4-b3b9-dfc66427bbd0",
                "content-type": "text/xml",
                "content-length": "950",
                "date": "Wed, 31 May 2023 16:50:00 GMT",
            },
            "RetryAttempts": 0,
        },
    }
    IamHelper.set_cluster_identifier(MagicMock(), rp)

    assert rp.cluster_identifier == exp_cluster_identifier
