from redshift_connector import RedshiftProperty
from redshift_connector.config import ClientProtocolVersion


def make_redshift_property() -> RedshiftProperty:
    rp: RedshiftProperty = RedshiftProperty()
    rp.user_name = "mario@luigi.com"
    rp.password = "bowser"
    rp.db_name = "dev"
    rp.cluster_identifier = "something"
    rp.idp_host = "8000"
    rp.duration = 100
    rp.preferred_role = "analyst"
    rp.ssl_insecure = False
    rp.db_user = "primary"
    rp.db_groups = ["employees"]
    rp.force_lowercase = True
    rp.auto_create = False
    rp.region = "us-west-1"
    rp.principal = "arn:aws:iam::123456789012:user/Development/product_1234/*"
    rp.client_protocol_version = ClientProtocolVersion.BASE_SERVER
    return rp
