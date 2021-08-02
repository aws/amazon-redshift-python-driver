import typing
from test import db_kwargs

import pytest

import redshift_connector

access_key_id: str = "replace_me"
secret_access_key: str = "replace_me"
session_token: str = "replace_me"

"""
This is a manually executable test. It requires valid AWS credentials.

A Redshift authentication profile will be created prior to the test.
Following the test, this profile will be deleted.

Please modify the fixture, handle_redshift_auth_profile, to include any additional arguments to boto3 client required
for your testing configuration. This may include attributes such as endpoint_url and region. The contents of the auth
profile can be modified as needed.
"""


creds = {"aws_access_key_id": "replace_me", "aws_session_token": "replace_me", "aws_secret_access_key": "replace_me"}

auth_profile_name: str = "PythonManualTest"


@pytest.fixture(autouse=True)
def handle_redshift_auth_profile(request, db_kwargs):

    import json

    import boto3
    from botocore.exceptions import ClientError

    payload = json.dumps(
        {
            "host": db_kwargs["host"],
            "db_user": db_kwargs["user"],
            "max_prepared_statements": 5,
            "region": db_kwargs["region"],
            "cluster_identifier": db_kwargs["cluster_identifier"],
            "db_name": db_kwargs["database"],
        }
    )

    try:
        client = boto3.client(
            "redshift",
            **{**creds, **{"region_name": db_kwargs["region"]}},
            verify=False,
        )
        client.create_authentication_profile(
            AuthenticationProfileName=auth_profile_name, AuthenticationProfileContent=payload
        )
    except ClientError:
        raise

    def fin():
        import boto3
        from botocore.exceptions import ClientError

        try:
            client = boto3.client(
                "redshift",
                **{**creds, **{"region_name": db_kwargs["region"]}},
                verify=False,
            )
            client.delete_authentication_profile(
                AuthenticationProfileName=auth_profile_name,
            )
        except ClientError:
            raise

    request.addfinalizer(fin)


@pytest.mark.skip(reason="manual")
def test_redshift_auth_profile_can_connect(db_kwargs):

    with redshift_connector.connect(
        region=db_kwargs["region"],
        access_key_id=creds["aws_access_key_id"],
        secret_access_key=creds["aws_secret_access_key"],
        session_token=creds["aws_session_token"],
        auth_profile=auth_profile_name,
        iam=True,
    ) as conn:
        assert conn.user == "IAM:{}".format(db_kwargs["user"]).encode()
        assert conn.max_prepared_statements == 5
