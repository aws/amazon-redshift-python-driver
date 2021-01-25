import typing
from unittest.mock import MagicMock, patch

import boto3  # type: ignore
import pytest  # type: ignore
from pytest_mock import mocker

from redshift_connector import InterfaceError
from redshift_connector.auth.aws_credentials_provider import (
    AWSCredentialsProvider,
    AWSDirectCredentialsHolder,
)
from redshift_connector.credentials_holder import AWSProfileCredentialsHolder
from redshift_connector.redshift_property import RedshiftProperty


def _make_aws_credentials_obj_with_profile() -> AWSCredentialsProvider:
    cred_provider: AWSCredentialsProvider = AWSCredentialsProvider()
    rp: RedshiftProperty = RedshiftProperty()
    profile_name: str = "myProfile"

    rp.profile = profile_name

    cred_provider.add_parameter(rp)
    return cred_provider


def _make_aws_credentials_obj_with_credentials() -> AWSCredentialsProvider:
    cred_provider: AWSCredentialsProvider = AWSCredentialsProvider()
    rp: RedshiftProperty = RedshiftProperty()
    access_key_id: str = "my_access"
    secret_key: str = "my_secret"
    session_token: str = "my_session"

    rp.access_key_id = access_key_id
    rp.secret_access_key = secret_key
    rp.session_token = session_token

    cred_provider.add_parameter(rp)
    return cred_provider


def test_create_aws_credentials_provider_with_profile():
    cred_provider: AWSCredentialsProvider = _make_aws_credentials_obj_with_profile()
    assert cred_provider.profile == "myProfile"
    assert cred_provider.access_key_id is None
    assert cred_provider.secret_access_key is None
    assert cred_provider.session_token is None


def test_create_aws_credentials_provider_with_credentials():
    cred_provider: AWSCredentialsProvider = _make_aws_credentials_obj_with_credentials()
    assert cred_provider.profile is None
    assert cred_provider.access_key_id == "my_access"
    assert cred_provider.secret_access_key == "my_secret"
    assert cred_provider.session_token == "my_session"


def test_get_cache_key_with_profile():
    cred_provider: AWSCredentialsProvider = _make_aws_credentials_obj_with_profile()
    assert cred_provider.get_cache_key() == hash(cred_provider.profile)


def test_get_cache_key_with_credentials():
    cred_provider: AWSCredentialsProvider = _make_aws_credentials_obj_with_credentials()
    assert cred_provider.get_cache_key() == hash("my_access")


def test_get_credentials_checks_cache_first(mocker):
    mocked_credential_holder = MagicMock()

    def mock_set_cache(cp: AWSCredentialsProvider, key: str = "tomato") -> None:
        cp.cache[key] = mocked_credential_holder  # type: ignore

    cred_provider: AWSCredentialsProvider = AWSCredentialsProvider()
    mocker.patch("redshift_connector.auth.AWSCredentialsProvider.get_cache_key", return_value="tomato")

    with patch("redshift_connector.auth.AWSCredentialsProvider.refresh") as mocked_refresh:
        mocked_refresh.side_effect = mock_set_cache(cred_provider)
        get_cache_key_spy = mocker.spy(cred_provider, "get_cache_key")

        assert cred_provider.get_credentials() == mocked_credential_holder

        assert get_cache_key_spy.called is True
        assert get_cache_key_spy.call_count == 1


def test_get_credentials_refresh_error_is_raised(mocker):
    cred_provider: AWSCredentialsProvider = AWSCredentialsProvider()
    mocker.patch("redshift_connector.auth.AWSCredentialsProvider.get_cache_key", return_value="tomato")
    expected_exception = "something went wrong"

    with patch("redshift_connector.auth.AWSCredentialsProvider.refresh") as mocked_refresh:
        mocked_refresh.side_effect = Exception(expected_exception)

        with pytest.raises(InterfaceError, match=expected_exception):
            cred_provider.get_credentials()


def test_refresh_uses_profile_if_present(mocker):
    cred_provider: AWSCredentialsProvider = _make_aws_credentials_obj_with_profile()
    mocked_boto_session: MagicMock = MagicMock()

    with patch("boto3.Session", return_value=mocked_boto_session):
        cred_provider.refresh()

        assert hash("myProfile") in cred_provider.cache
        assert isinstance(cred_provider.cache[hash("myProfile")], AWSProfileCredentialsHolder)
        assert cred_provider.cache[hash("myProfile")].profile == "myProfile"


def test_refresh_uses_credentials_if_present(mocker):
    cred_provider: AWSCredentialsProvider = _make_aws_credentials_obj_with_credentials()
    mocked_boto_session: MagicMock = MagicMock()

    with patch("boto3.Session", return_value=mocked_boto_session):
        cred_provider.refresh()

        assert hash("my_access") in cred_provider.cache
        assert isinstance(cred_provider.cache[hash("my_access")], AWSDirectCredentialsHolder)
        assert cred_provider.cache[hash("my_access")].access_key_id == "my_access"
        assert cred_provider.cache[hash("my_access")].secret_access_key == "my_secret"
        assert cred_provider.cache[hash("my_access")].session_token == "my_session"
