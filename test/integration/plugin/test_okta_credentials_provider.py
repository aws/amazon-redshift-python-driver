import configparser
import os

import pytest  # type: ignore

import redshift_connector

conf = configparser.ConfigParser()
root_path = os.path.dirname(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir))))
conf.read(root_path + "/config.ini")

PROVIDER = ["okta_idp"]


@pytest.mark.parametrize("idp_arg", PROVIDER, indirect=True)
def test_idp_host_invalid_should_fail(idp_arg):
    wrong_idp_host: str = "andrew.okta.com"
    idp_arg["idp_host"] = wrong_idp_host

    with pytest.raises(redshift_connector.InterfaceError, match="Unauthorized"):
        redshift_connector.connect(**idp_arg)


@pytest.mark.parametrize("idp_arg", PROVIDER, indirect=True)
def test_preferred_role_should_use(idp_arg):
    idp_arg["preferred_role"] = conf.get("okta-idp", "preferred_role")
    with redshift_connector.connect(**idp_arg):
        pass
