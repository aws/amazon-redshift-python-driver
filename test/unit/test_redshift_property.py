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
def test_is_serverless_host(host, exp_is_serverless) -> None:
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
def test_set_serverless_acct_id_from_host(host, exp_account_id) -> None:
    info: RedshiftProperty = RedshiftProperty()
    info.host = host
    info.set_serverless_acct_id()
    assert info.serverless_acct_id == exp_account_id


@pytest.mark.parametrize(
    "host, exp_region",
    [
        ("testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com", "us-east-2"),
        ("123456789012.us-south-1.redshift-serverless.amazonaws.com", "us-south-1"),
        ("testwg2.012345678901.ap-northeast-3.redshift-serverless.amazonaws.com", "ap-northeast-3"),
        ("redshift-cluster-1.aaaaaaaaaaaa.us-east-2.redshift.amazonaws.com", "us-east-2"),
        ("mylongredshiftclustername.aaaaaaaaaaaa.ap-northeast-1.redshift.amazonaws.com", "ap-northeast-1"),
    ],
)
def test_set_region_from_host(host, exp_region) -> None:
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
def test_set_serverless_work_group_from_host(host, exp_work_group) -> None:
    info: RedshiftProperty = RedshiftProperty()
    info.host = host
    info.set_serverless_work_group_from_host()
    assert info.serverless_work_group == exp_work_group


@pytest.mark.parametrize(
    "host, exp_is_provisioned",
    [
        # serverless
        ("testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com", False),
        ("123456789012.us-south-1.redshift-serverless.amazonaws.com", False),
        ("testwg2.012345678901.ap-northeast-3.redshift-serverless.amazonaws.com", False),
        ("test-endpoint-j4n0p8vsprjvmpf4kc7h.123456789123.us-east-2-dev.redshift-serverless.amazonaws.com", False),
        # NLB
        ("nlbjdbc-NLB-b34deba1736c83fe.elb.us-east-1.amazonaws.com", False),
        # provisioned
        ("redshift-cluster-1.aaaaaaaaaaaa.us-east-2.redshift.amazonaws.com", True),
        ("redshift-cluster-1.aaaaaaaaaaaa.us-east-1-dev.redshift.amazonaws.com", True),
        # custom domain name
        ("my.custom.domain.name", False),
        ("redshift.company.prod.redshift.company.com", False),
        ("redshift.myproject.prod.redshift.amazonaws.mycompany.com", False),
    ],
)
def test_is_provisioned_host(host, exp_is_provisioned) -> None:
    rp: RedshiftProperty = RedshiftProperty()
    rp.put(key="host", value=host)

    assert rp.is_provisioned_host == exp_is_provisioned


@pytest.mark.parametrize(
    "conn_params, exp_is_cname",
    [
        # serverless
        ({"host": "testwg1.012345678901.us-east-2.redshift-serverless.amazonaws.com"}, False),
        ({"host": "123456789012.us-south-1.redshift-serverless.amazonaws.com"}, False),
        ({"host": "testwg2.012345678901.ap-northeast-3.redshift-serverless.amazonaws.com"}, False),
        ({"host": "012345678901.ap-northeast-3.redshift-serverless.amazonaws.com"}, False),
        (
            {"host": "test-endpoint-j4n0p8vsprjvmpf4kc7h.123456789123.us-east-2-dev.redshift-serverless.amazonaws.com"},
            False,
        ),
        # serverless NLB
        (
            {
                "host": "nlbjdbc-NLB-b34deba1736c83fe.elb.us-east-1.amazonaws.com",
                "is_serverless": True,
                "serverless_work_group": "xyz",
            },
            False,
        ),
        # serverless custom cluster name
        ({"host": "my.custom.domain.name", "is_serverless": True}, True),
        ({"host": "redshift.company.prod.redshift.company.com", "is_serverless": True}, True),
        ({"host": "redshift.myproject.prod.redshift.amazonaws.mycompany.com", "is_serverless": True}, True),
        # provisioned NLB
        (
            {
                "host": "nlbjdbc-NLB-b34deba1736c83fe.elb.us-east-1.amazonaws.com",
                "cluster_identifier": "my-nlb-cluster",
            },
            True,  # call is made but fails and falls back to using NLB logic
        ),
        # provisioned
        ({"host": "redshift-cluster-1.aaaaaaaaaaaa.us-east-2.redshift.amazonaws.com"}, False),
        ({"host": "redshift-cluster-1.aaaaaaaaaaaa.us-east-2-dev.redshift.amazonaws.com"}, False),
        # provisioned custom cluster name
        ({"host": "my.custom.domain.name"}, True),
        ({"host": "redshift.company.prod.redshift.company.com"}, True),
        ({"host": "redshift.myproject.prod.redshift.amazonaws.mycompany.com"}, True),
    ],
)
def test_set_is_cname_from_host(conn_params, exp_is_cname) -> None:
    info: RedshiftProperty = RedshiftProperty()

    for key, value in conn_params.items():
        info.put(key, value)

    info.set_is_cname()
    assert info.is_cname == exp_is_cname


def test_login_to_rp_exists() -> None:
    info: RedshiftProperty = RedshiftProperty()

    assert info.__getattribute__("login_to_rp")


def test_can_set_login_to_rp() -> None:
    info: RedshiftProperty = RedshiftProperty(**{"login_to_rp": "foo_bar"})

    assert info.__getattribute__("login_to_rp") == "foo_bar"
