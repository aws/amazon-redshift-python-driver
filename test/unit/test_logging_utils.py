import pytest

from redshift_connector import RedshiftProperty
from redshift_connector.utils.logging_utils import mask_secure_info_in_props

secret_rp_values = ("password", "access_key_id", "session_token", "secret_access_key")


@pytest.mark.parametrize("rp_arg", secret_rp_values)
def test_mask_secure_info_in_props_obscures_secret_value(rp_arg):
    rp: RedshiftProperty = RedshiftProperty()
    secret_value: str = "SECRET_VALUE"
    rp.put(rp_arg, secret_value)
    result = mask_secure_info_in_props(rp)
    assert result.__getattribute__(rp_arg) != secret_value


def test_mask_secure_info_in_props_no_info():
    assert mask_secure_info_in_props(None) is None
