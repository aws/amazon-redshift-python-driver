import pytest

import redshift_connector

aws_secret_access_key: str = ""
aws_access_key: str = ""
aws_session_token: str = ""

"""
How to use:
0) If necessary, create a Redshift cluster
1) In the connect method below,  specify the connection parameters
3) Specify the AWS IAM credentials in the variables above
4) Manually execute this test
"""


@pytest.mark.skip(reason="manual")
def test_use_aws_credentials_default_profile():
    with redshift_connector.connect(
        iam=True,
        database="my_database",
        db_user="my_db_user",
        password="",
        user="",
        cluster_identifier="my_cluster_identifier",
        region="my_region",
        access_key_id=aws_access_key,
        secret_access_key=aws_secret_access_key,
        session_token=aws_session_token,
    ) as con:
        with con.cursor() as cursor:
            cursor.execute("select 1")
