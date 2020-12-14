import base64
import logging
import typing

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.credential_provider_constants import azure_headers
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


#  Class to get SAML Response from Microsoft Azure using OAuth 2.0 API
class AzureCredentialsProvider(SamlCredentialsProvider):
    def __init__(self: "AzureCredentialsProvider") -> None:
        super().__init__()
        self.idp_tenant: typing.Optional[str] = None
        self.client_secret: typing.Optional[str] = None
        self.client_id: typing.Optional[str] = None

    # method to grab the field parameters specified by end user.
    # This method adds to it Azure specific parameters.
    def add_parameter(self: "AzureCredentialsProvider", info: RedshiftProperty) -> None:
        super().add_parameter(info)
        # The value of parameter idp_tenant.
        self.idp_tenant = info.idp_tenant
        # The value of parameter client_secret.
        self.client_secret = info.client_secret
        # The value of parameter client_id.
        self.client_id = info.client_id

    # Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
    def get_saml_assertion(self: "AzureCredentialsProvider") -> str:
        # idp_tenant, client_secret, and client_id are
        # all required parameters to be able to authenticate with Microsoft Azure.
        # user and password are also required and need to be set to the username and password of the
        # Microsoft Azure account that is logging in.
        if self.user_name == "" or self.user_name is None:
            raise InterfaceError("Missing required property: user_name")
        if self.password == "" or self.password is None:
            raise InterfaceError("Missing required property: password")
        if self.idp_tenant == "" or self.idp_tenant is None:
            raise InterfaceError("Missing required property: idp_tenant")
        if self.client_secret == "" or self.client_secret is None:
            raise InterfaceError("Missing required property: client_secret")
        if self.client_id == "" or self.client_id is None:
            raise InterfaceError("Missing required property: client_id")

        return self.azure_oauth_based_authentication()

    #  Method to initiate a POST request to grab the SAML Assertion from Microsoft Azure
    #  and convert it to a SAML Response.
    def azure_oauth_based_authentication(self: "AzureCredentialsProvider") -> str:
        import requests

        # endpoint to connect with Microsoft Azure to get SAML Assertion token
        url: str = "https://login.microsoftonline.com/{tenant}/oauth2/token".format(tenant=self.idp_tenant)
        # headers to pass with POST request
        headers: typing.Dict[str, str] = azure_headers
        # required parameters to pass in POST body
        payload: typing.Dict[str, typing.Optional[str]] = {
            "grant_type": "password",
            "requested_token_type": "urn:ietf:params:oauth:token-type:saml2",
            "username": self.user_name,
            "password": self.password,
            "client_secret": self.client_secret,
            "client_id": self.client_id,
            "resource": self.client_id,
        }

        try:
            response: "requests.Response" = requests.post(
                url, data=payload, headers=headers, verify=self.do_verify_ssl_cert()
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            _logger.error("Request for authentication from Azure was unsuccessful. {}".format(str(e)))
            raise InterfaceError(e)
        except requests.exceptions.Timeout as e:
            _logger.error("A timeout occurred when requesting authentication from Azure")
            raise InterfaceError(e)
        except requests.exceptions.TooManyRedirects as e:
            _logger.error(
                "A error occurred when requesting authentication from Azure. Verify RedshiftProperties are correct"
            )
            raise InterfaceError(e)
        except requests.exceptions.RequestException as e:
            _logger.error("A unknown error occurred when requesting authentication from Azure.")
            raise InterfaceError(e)

        # parse the JSON response to grab access_token field which contains Base64 encoded SAML
        # Assertion and decode it
        saml_assertion: str = ""
        try:
            saml_assertion = response.json()["access_token"]
        except Exception as e:
            _logger.error("Failed to authenticate with Azure. Response from Azure did not include access_token.")
            raise InterfaceError(e)
        if saml_assertion == "":
            raise InterfaceError("Azure access_token is empty")

        missing_padding: int = 4 - len(saml_assertion) % 4
        if missing_padding:
            saml_assertion += "=" * missing_padding

        # decode the SAML Assertion to a String to add XML tags to form a SAML Response
        decoded_saml_assertion: str = ""
        try:
            decoded_saml_assertion = str(base64.urlsafe_b64decode(saml_assertion))
        except TypeError as e:
            _logger.error("Failed to decode saml assertion returned from Azure")
            raise InterfaceError(e)

        # SAML Response is required to be sent to base class. We need to provide a minimum of:
        # 1) samlp:Response XML tag with xmlns:samlp protocol value
        # 2) samlp:Status XML tag and samlpStatusCode XML tag with Value indicating Success
        # 3) followed by Signed SAML Assertion
        saml_response: str = (
            '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol">'
            "<samlp:Status>"
            '<samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>'
            "</samlp:Status>"
            "{decoded_saml_assertion}"
            "</samlp:Response>".format(decoded_saml_assertion=decoded_saml_assertion[2:-1])
        )

        # re-encode the SAML Response in Base64 and return this to the base class
        saml_response = str(base64.b64encode(saml_response.encode("utf-8")))[2:-1]

        return saml_response
