import base64
import concurrent.futures
import logging
import random
import socket
import webbrowser
from typing import Dict, List, Optional

import requests

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.CredentialProviderConstants import azure_headers
from redshift_connector.plugin.SamlCredentialsProvider import SamlCredentialsProvider
from redshift_connector.RedshiftProperty import RedshiftProperty

logger = logging.getLogger(__name__)


#  Class to get SAML Response from Microsoft Azure using OAuth 2.0 API
class BrowserAzureCredentialsProvider(SamlCredentialsProvider):

    def __init__(self) -> None:
        super().__init__()
        self.idp_tenant: Optional[str] = None
        self.client_secret: Optional[str] = None
        self.client_id: Optional[str] = None

        self.idp_response_timeout: int = 120
        self.listen_port: int = 7890

        self.redirectUri: Optional[str] = None

    # method to grab the field parameters from JDBC connection string.
    # This method adds to it Azure specific parameters.
    def add_parameter(self, info: RedshiftProperty) -> None:
        super().add_parameter(info)
        # The value of parameter idp_tenant.
        self.idp_tenant = info.idp_tenant
        # The value of parameter client_id.
        self.client_secret = info.client_secret
        self.client_id = info.client_id

        self.idp_response_timeout = info.idp_response_timeout
        self.listen_port = info.listen_port

    # Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
    def get_saml_assertion(self) -> str:

        if self.idp_tenant == '' or self.idp_tenant is None:
            raise InterfaceError("Missing required property: idp_tenant")
        if self.client_id == '' or self.client_id is None:
            raise InterfaceError("Missing required property: client_id")

        if self.idp_response_timeout < 10:
            raise InterfaceError("idp_response_timeout should be 10 seconds or greater.")
        if self.listen_port < 1 or self.listen_port > 65535:
            raise InterfaceError("Invalid property value: listen_port")

        self.redirectUri = "http://localhost:{port}/redshift/".format(port=self.listen_port)

        token: str = self.fetch_authorization_token()
        saml_assertion: str = self.fetch_saml_response(token)
        return self.wrap_and_encode_assertion(saml_assertion)

    #  First authentication phase:
    #  Set the state in order to check if the incoming request belongs to the current authentication process.
    #  Start the Socket Server at the {@linkplain BrowserAzureCredentialsProvider#m_listen_port} port.
    #  Open the default browser with the link asking a User to enter the credentials.
    #  Retrieve the SAML Assertion string from the response. Decode it, format, validate and return.
    def fetch_authorization_token(self) -> str:
        alphabet: str = 'abcdefghijklmnopqrstuvwxyz'
        state: str = ''.join(random.sample(alphabet, 10))
        try:
            return_value: str = ''
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(self.run_server, self.listen_port, self.idp_response_timeout, state)
                self.open_browser(state)
                return_value = future.result()

            return str(return_value)
        except socket.error as e:
            logger.error("socket error: %s", e)
            raise e
        except Exception as e:
            logger.error("other Exception: %s", e)
            raise e

    # Initiates the request to the IDP and gets the response body
    # Get Base 64 encoded saml assertion from the response body
    def fetch_saml_response(self, token):
        url: str = "https://login.microsoftonline.com/{tenant}/oauth2/token".format(tenant=self.idp_tenant)
        # headers to pass with POST request
        headers: Dict[str, str] = azure_headers
        # required parameters to pass in POST body
        payload: Dict[str, Optional[str]] = {
            "code": token,
            "requested_token_type": "urn:ietf:params:oauth:token-type:saml2",
            "grant_type": "authorization_code",
            "scope": "openid",
            "resource": self.client_id,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirectUri
        }
        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error("Request for authentication from Microsoft was unsuccessful. {}".format(str(e)))
            raise InterfaceError(e)
        except requests.exceptions.Timeout as e:
            logger.error("A timeout occurred when requesting authentication from Azure")
            raise InterfaceError(e)
        except requests.exceptions.TooManyRedirects as e:
            logger.error("A error occurred when requesting authentication from Azure. Verify RedshiftProperties are correct")
            raise InterfaceError(e)
        except requests.exceptions.RequestException as e:
            logger.error("A unknown error occurred when requesting authentication from Azure")
            raise InterfaceError(e)

        try:
            saml_assertion: str = response.json()['access_token']
        except TypeError as e:
            logger.error("Failed to decode saml assertion returned from Azure")
            raise InterfaceError(e)
        if saml_assertion == '':
            raise InterfaceError("Azure access_token is empty")

        missing_padding: int = 4 - len(saml_assertion) % 4
        if missing_padding:
            saml_assertion += '=' * missing_padding

        return str(base64.urlsafe_b64decode(saml_assertion))

    # SAML Response is required to be sent to base class. We need to provide a minimum of:
    # samlp:Response XML tag with xmlns:samlp protocol value
    # samlp:Status XML tag and samlpStatusCode XML tag with Value indicating Success
    # followed by Signed SAML Assertion
    def wrap_and_encode_assertion(self, saml_assertion: str) -> str:
        saml_response: str = "<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\">" \
                        "<samlp:Status>" \
                        "<samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>" \
                        "</samlp:Status>" \
                        "{saml_assertion}" \
                        "</samlp:Response>".format(saml_assertion=saml_assertion[2:-1])

        return str(base64.b64encode(saml_response.encode('utf-8')))[2:-1]

    def run_server(self, listen_port: int, idp_response_timeout: int, state: str) -> str:
        HOST: str = '127.0.0.1'
        PORT: int = listen_port

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen()
            conn, addr = s.accept()
            conn.settimeout(float(idp_response_timeout))
            size: int = 102400
            with conn:
                while True:
                    part: bytes = conn.recv(size)
                    if 'state' in part.decode():
                        data: List[str] = part.decode().split('&')
                        dic: Dict[str, str] = {}
                        for i in data:
                            info: List[str] = i.split('=')
                            dic[info[0]] = info[1]

                        if dic['state'] != state:
                            raise InterfaceError("Incoming state" + dic['state']+
                                                 " does not match the outgoing state " + state)
                        code: str = dic['code']
                        if code == '' or code is None:
                            raise InterfaceError("No valid code found")

                        return code

    # Opens the default browser with the authorization request to the IDP
    def open_browser(self, state: str) -> None:
        url: str = 'https://login.microsoftonline.com/{tenant}'\
              '/oauth2/authorize' \
              '?scope=openid' \
              '&response_type=code' \
              '&response_mode=form_post' \
              '&client_id={id}'\
              '&redirect_uri={uri}'\
              '&state={state}'.format(tenant=self.idp_tenant,id=self.client_id,uri=self.redirectUri,state=state)

        webbrowser.open(url)
