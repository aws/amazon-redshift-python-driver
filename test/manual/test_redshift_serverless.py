import pytest

import redshift_connector

"""
These functional tests ensure connections to Redshift serverless can be established when
using various authentication methods.

Please note the pre-requisites were documented while this feature is under public preview,
and are subject to change.

Pre-requisites:
1) Redshift serverless configuration
2) EC2 instance configured for accessing Redshift serverless (i.e. in compatible VPC, subnet)
3) Perform a sanity check using psql to ensure Redshift serverless connection can be established
3) EC2 instance has Python installed
4) Clone redshift_connector on EC2 instance and install

How to use:
1) Populate config.ini with the Redshift serverless endpoint and user authentication information
2) Run this file with pytest
"""


@pytest.mark.skip(reason="manual")
def test_native_auth(serverless_native_db_kwargs):
    with redshift_connector.connect(**serverless_native_db_kwargs):
        pass


@pytest.mark.skip(reason="manual")
def test_iam_auth(serverless_iam_db_kwargs):
    with redshift_connector.connect(**serverless_iam_db_kwargs):
        pass


@pytest.mark.skip(reason="manual")
def test_idp_auth(okta_idp):
    okta_idp["host"] = "my_redshift_serverless_endpoint"

    with redshift_connector.connect(**okta_idp):
        pass
