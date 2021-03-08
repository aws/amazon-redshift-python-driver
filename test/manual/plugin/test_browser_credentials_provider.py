import configparser
import os
import typing
from test import (
    azure_browser_idp,
    idp_arg,
    jumpcloud_browser_idp,
    jwt_azure_v2_idp,
    jwt_google_idp,
    okta_browser_idp,
    ping_browser_idp,
)

import pytest  # type: ignore

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir))))
conf.read(root_path + "/config.ini")

BROWSER_CREDENTIAL_PROVIDERS: typing.List[str] = [
    "jumpcloud_browser_idp",
    "okta_browser_idp",
    "azure_browser_idp",
    "jwt_azure_v2_idp",
    "jwt_google_idp",
    "ping_browser_idp",
]

"""
How to use:
0) If necessary, create a Redshift cluster and configure it for use with the desired IdP
1) In config.ini specify the connection parameters required by the desired IdP fixture in test/integration/conftest.py
2) Ensure browser cookies have been cleared
3) Manually execute the tests in this file, providing the necessary login information in the web browser
"""


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("idp_arg", BROWSER_CREDENTIAL_PROVIDERS, indirect=True)
def test_browser_credentials_provider_can_auth(idp_arg):
    with redshift_connector.connect(**idp_arg):
        pass
