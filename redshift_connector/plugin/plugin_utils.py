import typing

from redshift_connector.error import InterfaceError

# Microsoft IdP host constants
MICROSOFT_COMMERCIAL_HOST = "login.microsoftonline.com"
MICROSOFT_IDP_HOSTS = {"us-gov": "login.microsoftonline.us", "cn": "login.chinacloudapi.cn"}


def get_microsoft_idp_host(idp_partition: typing.Optional[str] = None) -> str:
    """Returns the appropriate Microsoft IDP host based on the idp_partition value."""
    from redshift_connector.plugin.azure_utils import validate_idp_partition

    validated_partition = validate_idp_partition(idp_partition)
    if not validated_partition or not validated_partition.strip():
        return MICROSOFT_COMMERCIAL_HOST

    partition = validated_partition.strip().lower()
    
    if partition == "commercial":
        return MICROSOFT_COMMERCIAL_HOST
    
    if partition in MICROSOFT_IDP_HOSTS:
        return MICROSOFT_IDP_HOSTS[partition]
    else:
        supported_values = list(MICROSOFT_IDP_HOSTS.keys()) + ["commercial"]
        raise InterfaceError("Invalid IdP partition: '{}' (normalized: '{}'). Supported values are: {}, or empty/None for commercial cloud.".format(
            idp_partition, partition, ", ".join("'{}'".format(v) for v in supported_values)
        ))
