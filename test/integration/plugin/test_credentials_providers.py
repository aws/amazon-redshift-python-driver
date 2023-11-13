import configparser
import os
import sys
import typing

import botocore  # type: ignore
import pytest  # type: ignore

import redshift_connector
from redshift_connector import DriverInfo

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir))))
conf.read(root_path + "/config.ini")


NON_BROWSER_IDP: typing.List[str] = ["okta_idp", "azure_idp"]  # TODO: disabling "adfs_idp" tests due to CI issue
ALL_IDP: typing.List[str] = ["okta_browser_idp", "azure_browser_idp"] + NON_BROWSER_IDP


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


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_idp_password(idp_arg):
    idp_arg["password"] = "wrong_password"

    with pytest.raises(
        redshift_connector.InterfaceError,
        match=r"(Failed to get SAML assertion)",
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_cluster_identifier(idp_arg):
    wrong_identifier = "redshift-cluster-11"
    idp_arg["cluster_identifier"] = wrong_identifier

    with pytest.raises(botocore.exceptions.ClientError, match="Cluster {} not found.".format(wrong_identifier)):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_region(idp_arg):
    wrong_region = "us-east-22"
    idp_arg["region"] = wrong_region

    with pytest.raises(
        botocore.exceptions.EndpointConnectionError,
        match='Could not connect to the endpoint URL: "https://redshift.{}.amazonaws.com/"'.format(wrong_region),
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_credentials_provider(idp_arg):
    with redshift_connector.connect(**idp_arg):
        pass


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_preferred_role_invalid_should_fail(idp_arg):
    idp_arg["preferred_role"] = "arn:aws:iam::111111111111:role/Trash-role"
    with pytest.raises(
        redshift_connector.InterfaceError,
        match="User specified preferred_role was not found in SAML assertion https://aws.amazon.com/SAML/Attributes/Role Attribute",
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.skip(reason="flaky")
@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_invalid_db_group(idp_arg):
    import botocore.exceptions

    idp_arg["db_groups"] = ["girl_dont_do_it"]
    with pytest.raises(
        expected_exception=(redshift_connector.ProgrammingError, botocore.exceptions.ClientError),
        match="{}".format(idp_arg["db_groups"][0]),
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", ["okta_idp", "azure_idp"], indirect=True)
@pytest.mark.parametrize("ssl_insecure_val", [True, False])
def test_ssl_insecure_is_used(idp_arg, ssl_insecure_val):
    idp_arg["ssl_insecure"] = ssl_insecure_val

    with redshift_connector.connect(**idp_arg):
        pass


@pytest.mark.parametrize("idp_arg", ALL_IDP, indirect=True)
def testSslAndIam(idp_arg):
    idp_arg["ssl"] = False
    idp_arg["iam"] = True
    with pytest.raises(
        redshift_connector.InterfaceError,
        match="Invalid connection property setting",
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", ALL_IDP, indirect=True)
def test_invalid_credentials_provider_should_raise(idp_arg):
    idp_arg["iam"] = False
    idp_arg["credentials_provider"] = "OktacredentialSProvider"
    with pytest.raises(
        redshift_connector.InterfaceError,
        match="Invalid IdP specified in credential_provider connection parameter",
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", ALL_IDP, indirect=True)
def testWrongCredentialsProvider(idp_arg):
    idp_arg["credentials_provider"] = "WrongProvider"
    with pytest.raises(
        redshift_connector.InterfaceError, match="Invalid IdP specified in credential_provider connection paramete"
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def use_cached_temporary_credentials(idp_arg):
    # ensure nothing is in the credential cache
    redshift_connector.IamHelper.credentials_cache.clear()

    with redshift_connector.connect(**idp_arg):
        pass

    assert len(redshift_connector.IamHelper.credentials_cache) == 1
    first_cred_cache_entry = redshift_connector.IamHelper.credentials_cache.popitem()

    with redshift_connector.connect(**idp_arg):
        pass

    # we should have used the temporary credentials retrieved in first AWS API call, verify cache still
    # holds these
    assert len(redshift_connector.IamHelper.credentials_cache) == 1
    assert first_cred_cache_entry == redshift_connector.IamHelper.credentials_cache.popitem()


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_stl_connection_log_contains_plugin_name(idp_arg, db_kwargs):
    idp_arg["auto_create"] = True
    with redshift_connector.connect(**idp_arg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "select top 1 1 from stl_connection_log where driver_version = %s and plugin_name like %s",
                (
                    DriverInfo.driver_full_name(),
                    "redshift_connector.plugin.{}".format(idp_arg["credentials_provider"])[:32],
                ),
            )
            res = cursor.fetchone()
            assert res is not None
            assert res[0] == 1


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def raise_exception_when_uppercase_db_groups(idp_arg, db_groups):
    idp_arg["db_groups"] = [group.upper() for group in db_groups]

    with pytest.raises():
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def uses_force_lowercase_when_db_groups_uppercase(idp_arg, db_groups):
    idp_arg["db_groups"] = [group.upper() for group in db_groups]
    idp_arg["force_lowercase"] = True

    with redshift_connector.connect(**idp_arg):
        pass


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def uses_db_groups_nominal(idp_arg, db_groups):
    idp_arg["db_groups"] = [group for group in db_groups]

    with redshift_connector.connect(**idp_arg):
        pass


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_connect_with_group_federation(idp_arg):
    idp_arg["group_federation"] = True

    with redshift_connector.connect(**idp_arg):
        pass
