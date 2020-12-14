import concurrent.futures
import logging
import re
import socket
import typing
import urllib.parse

from redshift_connector.error import InterfaceError
from redshift_connector.plugin.saml_credentials_provider import SamlCredentialsProvider
from redshift_connector.redshift_property import RedshiftProperty

_logger: logging.Logger = logging.getLogger(__name__)


#  Class to get SAML Response
class BrowserSamlCredentialsProvider(SamlCredentialsProvider):
    def __init__(self: "BrowserSamlCredentialsProvider") -> None:
        super().__init__()
        self.login_url: typing.Optional[str] = None

        self.idp_response_timeout: int = 120
        self.listen_port: int = 7890

    # method to grab the field parameters specified by end user.
    # This method adds to it specific parameters.
    def add_parameter(self: "BrowserSamlCredentialsProvider", info: RedshiftProperty) -> None:
        super().add_parameter(info)
        self.login_url = info.login_url

        self.idp_response_timeout = info.idp_response_timeout
        self.listen_port = info.listen_port

    # Required method to grab the SAML Response. Used in base class to refresh temporary credentials.
    def get_saml_assertion(self: "BrowserSamlCredentialsProvider") -> str:

        if self.login_url == "" or self.login_url is None:
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
    def authenticate(self: "BrowserSamlCredentialsProvider") -> str:
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(self.run_server, self.listen_port, self.idp_response_timeout)
                self.open_browser()
                return_value: str = future.result()

            samlresponse = urllib.parse.unquote(return_value)
            return str(samlresponse)
        except socket.error as e:
            _logger.error("socket error: %s", e)
            raise e
        except Exception as e:
            _logger.error("other Exception: %s", e)
            raise e

    def run_server(self: "BrowserSamlCredentialsProvider", listen_port: int, idp_response_timeout: int) -> str:
        HOST: str = "127.0.0.1"
        PORT: int = listen_port

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen()
            conn, addr = s.accept()  # typing.Tuple[Socket, Any]
            conn.settimeout(float(idp_response_timeout))
            size: int = 102400
            with conn:
                while True:
                    part: bytes = conn.recv(size)
                    decoded_part: str = part.decode()
                    result: typing.Optional[typing.Match] = re.search(
                        pattern="SAMLResponse[:=]+[\\n\\r]*", string=decoded_part, flags=re.MULTILINE
                    )

                    if result is not None:
                        saml_resp_block: str = decoded_part[result.regs[0][1] :]
                        end_idx: int = saml_resp_block.find("&RelayState=")
                        if end_idx > -1:
                            saml_resp_block = saml_resp_block[:end_idx]
                        return saml_resp_block

    # Opens the default browser with the authorization request to the web service.
    def open_browser(self: "BrowserSamlCredentialsProvider") -> None:
        import webbrowser

        url: typing.Optional[str] = self.login_url
        if url is None:
            raise InterfaceError("the login_url could not be empty")
        webbrowser.open(url)
