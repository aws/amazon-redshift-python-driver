import configparser
import os
import sys
import typing

import botocore
import pytest  # type: ignore

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir))))
conf.read(root_path + "/config.ini")


NON_BROWSER_IDP: typing.List[str] = ["okta_idp"]

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


@pytest.fixture
def idp_arg(request):
    return request.getfixturevalue(request.param)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def testIdpPassword(idp_arg):
    idp_arg["password"] = "wrong_password"

    with pytest.raises(redshift_connector.InterfaceError, match="Unauthorized"):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def testIdpHost(idp_arg):
    wrong_idp_host = "andrew.okta.com"
    idp_arg["idp_host"] = wrong_idp_host

    with pytest.raises(redshift_connector.InterfaceError, match="Unauthorized"):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def testClusterIdentifier(idp_arg):
    wrong_identifier = "redshift-cluster-11"
    idp_arg["cluster_identifier"] = wrong_identifier

    with pytest.raises(botocore.exceptions.ClientError, match="Cluster {} not found.".format(wrong_identifier)):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def testRegion(idp_arg):
    wrong_region = "us-east-22"
    idp_arg["region"] = wrong_region

    with pytest.raises(
        botocore.exceptions.EndpointConnectionError,
        match='Could not connect to the endpoint URL: "https://redshift.{}.amazonaws.com/"'.format(wrong_region),
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def testCredentialsProvider(idp_arg):
    with redshift_connector.connect(**idp_arg):
        pass


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def testPreferredRole(idp_arg):
    idp_arg["preferred_role"] = conf.get("okta-idp", "preferred_role")
    with redshift_connector.connect(**idp_arg):
        pass

    idp_arg["preferred_role"] = "arn:aws:iam::111111111111:role/Okta-role"
    with pytest.raises(redshift_connector.InterfaceError, match="Preferred role not found in SamlAssertion"):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
def test_invalid_db_group(idp_arg):
    idp_arg["db_groups"] = ["girl_dont_do_it"]
    with pytest.raises(
        redshift_connector.ProgrammingError, match='Group "{}" does not exist'.format(idp_arg["db_groups"][0])
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", NON_BROWSER_IDP, indirect=True)
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
        match="Invalid connection property setting. SSL must be enabled when using IAM",
    ):
        redshift_connector.connect(**idp_arg)

    idp_arg["iam"] = False
    idp_arg["credentials_provider"] = "OktacredentialSProvider"
    with pytest.raises(
        redshift_connector.InterfaceError,
        match="Invalid connection property setting. IAM must be enabled when using credentials "
        "via identity provider",
    ):
        redshift_connector.connect(**idp_arg)

    idp_arg["ssl"] = True
    idp_arg["iam"] = True
    idp_arg["credentials_provider"] = None
    with pytest.raises(
        redshift_connector.InterfaceError,
        match="Invalid connection property setting. " "Credentials provider cannot be None when IAM is enabled",
    ):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", ALL_IDP, indirect=True)
def testWrongCredentialsProvider(idp_arg):
    idp_arg["credentials_provider"] = "WrongProvider"
    with pytest.raises(redshift_connector.InterfaceError, match="Invalid credentials provider" + "WrongProvider"):
        redshift_connector.connect(**idp_arg)
