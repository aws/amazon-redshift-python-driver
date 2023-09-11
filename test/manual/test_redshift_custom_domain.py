import pytest

import redshift_connector
from redshift_connector.idp_auth_helper import SupportedSSLMode

"""
These functional tests ensure connections to Redshift provisioned customer with custom domain name can be established
when using various authentication methods.

Pre-requisites:
1) Redshift provisioned configuration
2) Existing custom domain association with instance created in step 1
"""


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("sslmode", (SupportedSSLMode.VERIFY_CA, SupportedSSLMode.VERIFY_FULL))
def test_native_connect(provisioned_cname_db_kwargs, sslmode):
    # this test requires aws default profile contains valid credentials that provide permissions for
    # redshift:GetClusterCredentials ( Only called from this test method)
    import boto3

    profile = "default"
    client = boto3.client(
        service_name="redshift",
        region_name="eu-north-1",
    )
    # fetch cluster credentials and pass them as driver connect parameters
    response = client.get_cluster_credentials(
        CustomDomainName=provisioned_cname_db_kwargs["host"], DbUser=provisioned_cname_db_kwargs["db_user"]
    )

    provisioned_cname_db_kwargs["password"] = response["DbPassword"]
    provisioned_cname_db_kwargs["user"] = response["DbUser"]
    provisioned_cname_db_kwargs["profile"] = profile
    provisioned_cname_db_kwargs["ssl"] = True
    provisioned_cname_db_kwargs["sslmode"] = sslmode.value

    with redshift_connector.connect(**provisioned_cname_db_kwargs):
        pass


@pytest.mark.skip(reason="manual")
@pytest.mark.parametrize("sslmode", (SupportedSSLMode.VERIFY_CA, SupportedSSLMode.VERIFY_FULL))
def test_iam_connect(provisioned_cname_db_kwargs, sslmode):
    # this test requires aws default profile contains valid credentials that provide permissions for
    # redshift:GetClusterCredentials (called from driver)
    # redshift:DescribeClusters (called from driver)
    # redshift:DescribeCustomDomainAssociations (called from driver)
    provisioned_cname_db_kwargs["iam"] = True
    provisioned_cname_db_kwargs["profile"] = "default"
    provisioned_cname_db_kwargs["auto_create"] = True
    provisioned_cname_db_kwargs["ssl"] = True
    provisioned_cname_db_kwargs["sslmode"] = sslmode.value
    with redshift_connector.connect(**provisioned_cname_db_kwargs):
        pass


def test_idp_connect(okta_idp, provisioned_cname_db_kwargs):
    # todo
    pass


@pytest.mark.skip(reason="manual")
def test_nlb_connect():
    args = {
        "iam": True,
        # "access_key_id": "xxx",
        # "secret_access_key": "xxx",
        "cluster_identifier": "replace-me",
        "region": "us-east-1",
        "host": "replace-me",
        "port": 5439,
        "database": "dev",
        "db_user": "replace-me",
    }
    with redshift_connector.connect(**args):
        pass
