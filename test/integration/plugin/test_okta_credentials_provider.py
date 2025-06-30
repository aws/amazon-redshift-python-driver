import configparser
import os
import typing

import pytest  # type: ignore

import redshift_connector

conf: configparser.ConfigParser = configparser.ConfigParser()
root_path: str = os.path.dirname(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir))))
conf.read(root_path + "/config.ini")

PROVIDER: typing.List[str] = ["okta_idp"]


@pytest.mark.parametrize("idp_arg", PROVIDER, indirect=True)
def test_idp_host_invalid_should_fail(idp_arg) -> None:
    wrong_idp_host: str = "andrew.okta.com"
    idp_arg["idp_host"] = wrong_idp_host

    with pytest.raises(redshift_connector.InterfaceError, match="Failed to get SAML assertion"):
        redshift_connector.connect(**idp_arg)


@pytest.mark.skip(reason="Temporarily disable the test due to expired Okta credential (Redshift-115253)")
@pytest.mark.parametrize("idp_arg", PROVIDER, indirect=True)
def test_preferred_role_should_use(idp_arg) -> None:
    idp_arg["preferred_role"] = conf.get("okta-idp", "preferred_role")
    with redshift_connector.connect(**idp_arg):
        pass
