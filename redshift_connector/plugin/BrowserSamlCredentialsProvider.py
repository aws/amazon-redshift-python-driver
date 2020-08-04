import base64
import concurrent.futures
import logging
import socket
import urllib.parse
import webbrowser
from typing import Dict, List, Optional

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.SamlCredentialsProvider import SamlCredentialsProvider
from redshift_connector.RedshiftProperty import RedshiftProperty

logger = logging.getLogger(__name__)


#  Class to get SAML Response
class BrowserSamlCredentialsProvider(SamlCredentialsProvider):

    def __init__(self) -> None:
        super().__init__()
        self.login_url: Optional[str] = None

        self.idp_response_timeout: int = 120
        self.listen_port: int = 7890

    # method to grab the field parameters from JDBC connection string.
    # This method adds to it specific parameters.
    def add_parameter(self, info: RedshiftProperty) -> None:
        super().add_parameter(info)
        self.login_url = info.login_url

        self.idp_response_timeout = info.idp_response_timeout
        self.listen_port = info.listen_port

    # Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
    def get_saml_assertion(self) -> str:

        if self.login_url == '' or self.login_url is None:
            raise InterfaceError("Missing required property: login_url")

        if self.idp_response_timeout < 10:
            raise InterfaceError("idp_response_timeout should be 10 seconds or greater.")
        if self.listen_port < 1 or self.listen_port > 65535:
            raise InterfaceError("Invalid property value: listen_port")

        return self.authenticate()

    # Authentication consists of:
    # Start the Socket Server on the port {@link BrowserSamlCredentialsProvider#m_listen_port}.
    # Open the default browser with the link asking a User to enter the credentials.
    # Retrieve the SAML Assertion string from the response.
    def authenticate(self) -> str:
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(self.run_server, self.listen_port, self.idp_response_timeout)
                self.open_browser()
                return_value: str = future.result()

            samlresponse = urllib.parse.unquote(return_value)
            return str(samlresponse)
        except socket.error as e:
            logger.error("socket error: %s", e)
            raise e
        except Exception as e:
            logger.error("other Exception: %s", e)
            raise e

    def run_server(self, listen_port: int, idp_response_timeout: int) -> str:
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
                    if 'SAMLResponse' in part.decode():
                        data: List[str] = part.decode().split('&')
                        dic: Dict[str, str] = {}
                        for i in data:
                            info: List[str] = i.split('=')
                            dic[info[0]] = info[1]

                        return dic['SAMLResponse']

    # Opens the default browser with the authorization request to the web service.
    def open_browser(self) -> None:
        url: Optional[str] = self.login_url
        if url is None:
            raise InterfaceError("the login_url could not be empty")
        webbrowser.open(url)
