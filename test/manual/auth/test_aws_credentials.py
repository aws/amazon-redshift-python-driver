import pytest  # type: ignore

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
def test_use_aws_credentials_default_profile() -> None:
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


"""
How to use:
0) Generate credentials using instructions:  https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/getting-your-credentials.html
1) In the connect method below,  specify the connection parameters
3) Specify the AWS IAM credentials in the variables above
4) Update iam_helper.py to include correct min version.
5) Manually execute this test
"""


@pytest.mark.skip(reason="manual")
def test_use_get_cluster_credentials_with_iam(db_kwargs):
    role_name = "groupFederationTest"
    with redshift_connector.connect(**db_kwargs) as conn:
        with conn.cursor() as cursor:
            # https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html
            cursor.execute('create user "IAMR:{}" with password disable;'.format(role_name))
    with redshift_connector.connect(
        iam=True,
        database="replace_me",
        cluster_identifier="replace_me",
        region="replace_me",
        profile="replace_me",  # contains credentials for AssumeRole groupFederationTest
        group_federation=True,
    ) as con:
        with con.cursor() as cursor:
            cursor.execute("select 1")
            cursor.execute("select current_user")
            assert cursor.fetchone()[0] == role_name
