import pytest

import redshift_connector
from redshift_connector import RedshiftProperty


@pytest.mark.parametrize(
    "host, exp_is_serverless",
    [
        ("testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com", True),
        ("012345678901.us-east-2.redshift-serverless.amazonaws.com", True),
    ],
)
def test_is_serverless_host(host, exp_is_serverless):
    info: RedshiftProperty = RedshiftProperty()
    info.host = host
    assert info.is_serverless_host == exp_is_serverless


@pytest.mark.parametrize(
    "host, exp_account_id",
    [
        ("testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com", "012345678901"),
        ("testwg1234.123456789012.us-south-1.redshift-serverless.amazonaws.com", "123456789012"),
        ("012345678901.ap-northeast-3.redshift-serverless.amazonaws.com", "012345678901"),
    ],
)
def test_set_account_id_from_host(host, exp_account_id):
    info: RedshiftProperty = RedshiftProperty()
    info.host = host
    info.set_account_id_from_host()
    assert info.account_id == exp_account_id


@pytest.mark.parametrize(
    "host, exp_region",
    [
        ("testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com", "us-east-2"),
        ("123456789012.us-south-1.redshift-serverless.amazonaws.com", "us-south-1"),
        ("testwg2.012345678901.ap-northeast-3.redshift-serverless.amazonaws.com", "ap-northeast-3"),
    ],
)
def test_set_region_from_host(host, exp_region):
    info: RedshiftProperty = RedshiftProperty()
    info.host = host
    info.set_region_from_host()
    assert info.region == exp_region


@pytest.mark.parametrize(
    "host, exp_work_group",
    [
        ("testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com", "testwg1"),
        ("123456789012.us-south-1.redshift-serverless.amazonaws.com", None),
        ("testwg2.012345678901.ap-northeast-3.redshift-serverless.amazonaws.com", "testwg2"),
    ],
)
def test_set_work_group_from_host(host, exp_work_group):
    info: RedshiftProperty = RedshiftProperty()
    info.host = host
    info.set_work_group_from_host()
    assert info.work_group == exp_work_group
