import base64
import json
import typing
from test.unit.helpers import make_redshift_property
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from redshift_connector import RedshiftProperty
from redshift_connector.credentials_holder import CredentialsHolder
from redshift_connector.error import InterfaceError
from redshift_connector.plugin import (
    BasicJwtCredentialsProvider,
    JwtCredentialsProvider,
)


@patch.multiple(JwtCredentialsProvider, __abstractmethods__=set())
def make_jwtcredentialsprovider() -> JwtCredentialsProvider:
    return JwtCredentialsProvider()  # type: ignore


def test_make_jwtcredentialsprovider():
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    assert jwtcp.role_arn is None
    assert jwtcp.jwt is None
    assert jwtcp.role_session_name is JwtCredentialsProvider.DEFAULT_ROLE_SESSION_NAME
    assert jwtcp.duration is None
    assert jwtcp.db_user is None
    assert jwtcp.sts_endpoint is None
    assert jwtcp.region is None


def test_jwtcredentialsprovider_add_parameter():
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    rp: RedshiftProperty = make_redshift_property()

    _wit: str = "hooplah"
    _duration: int = 1234
    _role: str = "my_role"
    _session: str = "my_session"
    _region: str = "something"

    rp.role_arn = _role
    rp.web_identity_token = _wit
    rp.role_session_name = _session
    rp.duration = _duration
    rp.region = _region

    jwtcp.add_parameter(rp)
    assert jwtcp.jwt == _wit
    assert jwtcp.duration == _duration
    assert jwtcp.role_arn == _role
    assert jwtcp.role_session_name == _session
    assert jwtcp.region == _region


cache_key_vals: typing.List[typing.Tuple] = [("a", "b", "c", "d"), ()]


@pytest.mark.parametrize("_input", cache_key_vals)
def test_get_cache_key_no_attributes(_input):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    if len(_input) == 4:
        jwtcp.role_arn = _input[0]
        jwtcp.jwt = _input[1]
        jwtcp.role_session_name = _input[2]
        jwtcp.duration = _input[3]
        assert jwtcp.get_cache_key() == "".join(_input)
    else:
        assert jwtcp.get_cache_key() == "NoneNone{}None".format(JwtCredentialsProvider.DEFAULT_ROLE_SESSION_NAME)


@pytest.mark.parametrize("param", [JwtCredentialsProvider.KEY_ROLE_ARN, JwtCredentialsProvider.KEY_WEB_IDENTITY_TOKEN])
@pytest.mark.parametrize("invalid_val", [None, ""])
def test_check_required_parameters_missing_raises_exception(param, invalid_val):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    valid_val: str = "hello world!"

    if param == JwtCredentialsProvider.KEY_ROLE_ARN:
        jwtcp.role_arn = invalid_val
        jwtcp.jwt = valid_val
    elif param == JwtCredentialsProvider.KEY_WEB_IDENTITY_TOKEN:
        jwtcp.role_arn = valid_val
        jwtcp.jwt = invalid_val
    else:
        raise pytest.PytestWarning("Invalid arg supplied for param: {}".format(param))

    with pytest.raises(InterfaceError, match="Missing required property: {}".format(param)):
        jwtcp.check_required_parameters()


def make_jwt(
    v1: str, v2: str, v3: typing.Optional[str]
) -> typing.Tuple[typing.Optional[typing.List[typing.Union[bytes, str]]], str]:
    input_val: str = ""

    for _input in (v1, v2, v3):
        if _input is None:
            continue
        encoded_input: str = str(base64.b64encode(_input.encode("ascii")), "ascii")
        input_val += "{}\\.".format(encoded_input)

    exp_result: typing.Optional[typing.List[typing.Union[str, bytes]]] = None

    if all((v1, v2, v3)):
        exp_result = [
            v1.encode("ascii"),
            v2.encode("ascii"),
            str(base64.b64encode(v3.encode("ascii")), "ascii"),  # type: ignore
        ]

    return exp_result, input_val[:-2]


@pytest.mark.parametrize(
    "_input",
    [None, make_jwt("abc", "def", "ghi"), make_jwt("hithere", "hello", "goodbye"), make_jwt("hello", "world", None)],
)
def test_decode_jwt(_input):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    if _input is None:
        assert jwtcp.decode_jwt(_input) is None
    else:
        exp_result, jwt = _input
        assert jwtcp.decode_jwt(jwt) == exp_result


@pytest.mark.parametrize("_input", ["get_saml_assertion", "do_verify_ssl_cert", "get_form_action"])
def test_get_saml_assertion_not_implemented(_input):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    method_to_call = jwtcp.__getattribute__(_input)

    with pytest.raises(NotImplementedError):
        try:
            method_to_call()
        except TypeError:
            method_to_call("trash")


def test_get_credentials_handles_exception(mocker):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    mock_error_msg: str = "bad robot"
    with patch("redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.refresh") as buggy_refresh:
        buggy_refresh.side_effect = Exception(mock_error_msg)

        with pytest.raises(InterfaceError, match=mock_error_msg):
            jwtcp.get_credentials()


def test_get_credentials_returns_credentials(mocker):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    mock_cache_key = MagicMock()
    mock_cred_provider = MagicMock()

    def mock_set_cache(key, val):
        jwtcp.cache[key] = val

    mocker.patch(
        "redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.get_cache_key",
        return_value=mock_cache_key,
    )
    with patch("redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.refresh") as mock_refresh:
        mock_refresh.side_effect = mock_set_cache(mock_cache_key, mock_cred_provider)
        assert jwtcp.get_credentials() == mock_cred_provider


def test_get_credentials_none_found_raises_exception(mocker):
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()

    mocker.patch(
        "redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.get_cache_key",
        return_value=MagicMock(),
    )
    mocker.patch("redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.refresh")
    with pytest.raises(InterfaceError, match="Unable to load AWS credentials from IDP"):
        jwtcp.get_credentials()


def test_refresh_no_jwt():
    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()

    with pytest.raises(InterfaceError, match="no jwt provided"):
        jwtcp.refresh()


def test_refresh_passes_jwt_to_boto3(mocker):
    mocked_botocore_client = MagicMock()

    mocked_processed_jwt: str = "processed value"
    mocker.patch("boto3.client", return_value=mocked_botocore_client)
    mocker.patch(
        "redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.process_jwt",
        return_value=mocked_processed_jwt,
    )
    mocker.patch(
        "redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.decode_jwt", return_value=None
    )
    mocker.patch(
        "redshift_connector.plugin.jwt_credentials_provider.JwtCredentialsProvider.derive_database_user",
        return_value="Mouse",
    )

    jwtcp: JwtCredentialsProvider = make_jwtcredentialsprovider()
    mocked_orig_jwt: str = "initial value"
    mocked_role_arn: str = "my_role"
    jwtcp.jwt = mocked_orig_jwt
    jwtcp.role_arn = mocked_role_arn
    process_jwt_spy = mocker.spy(jwtcp, "process_jwt")
    decode_jwt_spy = mocker.spy(jwtcp, "decode_jwt")
    boto_spy = mocker.spy(mocked_botocore_client, "assume_role_with_web_identity")

    jwtcp.refresh()
    assert process_jwt_spy.called is True
    assert process_jwt_spy.call_count == 1
    assert process_jwt_spy.call_args[0][0] == mocked_orig_jwt

    assert decode_jwt_spy.called is True
    assert decode_jwt_spy.call_count == 1
    assert decode_jwt_spy.call_args[0][0] == mocked_orig_jwt

    assert boto_spy.called is True
    assert boto_spy.call_count == 1
    assert boto_spy.call_args[1]["RoleArn"] == mocked_role_arn
    assert boto_spy.call_args[1]["RoleSessionName"] == JwtCredentialsProvider.DEFAULT_ROLE_SESSION_NAME
    assert boto_spy.call_args[1]["WebIdentityToken"] == mocked_processed_jwt

    assert len(jwtcp.cache) == 1
    assert jwtcp.get_cache_key() in jwtcp.cache
    assert isinstance(jwtcp.cache[jwtcp.get_cache_key()], CredentialsHolder)


test_jwt_resp_data: typing.List[str] = [
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2MTgyNTgzNjQsImV4cCI6MTY0OTc5NDM2NCwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsIkdpdmVuTmFtZSI6IkpvaG5ueSIsIlN1cm5hbWUiOiJSb2NrZXQiLCJFbWFpbCI6Impyb2NrZXRAZXhhbXBsZS5jb20iLCJSb2xlIjpbIk1hbmFnZXIiLCJQcm9qZWN0IEFkbWluaXN0cmF0b3IiXX0.4lCP0ZgrKo3f6lQ9AtMdFEeDD5fBnszN3Deo18VyJ-o"
]


@pytest.mark.parametrize("encoded_jwt", test_jwt_resp_data)
def test_decode_jwt_resp(encoded_jwt):
    bjwtcp: BasicJwtCredentialsProvider = BasicJwtCredentialsProvider()

    decoded = bjwtcp.decode_jwt(encoded_jwt)
    assert len(decoded) == 3
    for idx, entry in enumerate(decoded[0:2]):
        str_entry = entry.decode("utf-8")
        data = json.loads(str_entry)
        assert len(data) > 1

        if idx == 0:
            for exp_key in ("alg", "typ"):
                assert exp_key in data
        elif idx == 1:
            for exp_key in ("iss", "iat", "exp", "aud", "sub", "GivenName", "Surname", "Email", "Role"):
                assert exp_key in data


@pytest.mark.parametrize("db_param", ["DbUser", "upn", "preferred_username", "email"])
def test_derive_database_user(db_param):
    data: typing.Dict[str, typing.Union[str, int]] = {
        "iss": "Online JWT Builder",
        "iat": 1618258364,
        "exp": 1649794364,
        "aud": "www.example.com",
        "sub": "jrocket@example.com",
        "GivenName": "Johnny",
        "Surname": "Rocket",
        "Role": ["Manager", "Project Administrator"],
    }
    DB_USER: str = "mr.bear@forest.com"
    data[db_param] = DB_USER

    mock_jwt_resp: typing.List[typing.Union[str, bytes]] = ["", json.dumps(data), "mocked resp"]

    bjwtcp: BasicJwtCredentialsProvider = BasicJwtCredentialsProvider()
    assert bjwtcp.derive_database_user(mock_jwt_resp) == DB_USER


@pytest.mark.parametrize(
    "decoded_data",
    [[""], ["" * 4], ["", json.dumps({"dbuser": "invalid"}), ""], ["", json.dumps({"Email": "invalid"}), ""]],
)
def test_derive_database_user_not_found(decoded_data):
    bjwtcp: BasicJwtCredentialsProvider = BasicJwtCredentialsProvider()

    with pytest.raises(InterfaceError):
        bjwtcp.derive_database_user(decoded_data)


def test_basic_jwt_credential_provider(mocker):
    bjwtcp: BasicJwtCredentialsProvider = BasicJwtCredentialsProvider()
    bjwtcp.jwt = "hi"
    bjwtcp.role_arn = "buttered bread role"

    checker_spy = mocker.spy(bjwtcp, "check_required_parameters")
    assert bjwtcp.process_jwt(bjwtcp.jwt) == bjwtcp.jwt
    assert checker_spy.called is True
    assert checker_spy.call_count == 1
