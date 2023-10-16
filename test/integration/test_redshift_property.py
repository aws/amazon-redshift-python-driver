import logging

import pytest

from redshift_connector import InterfaceError, RedshiftProperty

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_set_region_from_endpoint_lookup(db_kwargs) -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.put(key="host", value=db_kwargs["host"])
    rp.put(key="port", value=db_kwargs["port"])
    rp.set_region_from_host()

    expected_region = rp.region
    rp.region = None

    rp.set_region_from_endpoint_lookup()
    assert rp.region == expected_region


@pytest.mark.parametrize("host, port", [("x", 1000), ("amazon.com", -1), ("-o", 5439)])
def test_set_region_from_endpoint_lookup_raises(host, port, caplog) -> None:
    import logging

    rp: RedshiftProperty = RedshiftProperty()
    rp.put(key="host", value=host)
    rp.put(key="port", value=port)
    expected_msg: str = "Unable to automatically determine AWS region from host {} port {}. Please check host and port connection parameters are correct.".format(
        host, port
    )

    with caplog.at_level(logging.DEBUG):
        rp.set_region_from_endpoint_lookup()
    assert expected_msg in caplog.text
