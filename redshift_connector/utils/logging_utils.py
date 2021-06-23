import copy
import typing

if typing.TYPE_CHECKING:
    from redshift_connector import RedshiftProperty


def make_divider_block() -> str:
    return "=" * 35


def mask_secure_info_in_props(info: "RedshiftProperty") -> "RedshiftProperty":
    from redshift_connector import RedshiftProperty

    if info is None:
        return info
    secure_info_found: bool = False
    placeholder_value: str = "***"

    temp: RedshiftProperty = copy.deepcopy(info)

    def is_populated(field: typing.Optional[str]):
        return field is not None and field != ""

    if is_populated(temp.password):
        secure_info_found = True
        temp.password = placeholder_value
    if is_populated(temp.access_key_id):
        secure_info_found = True
        temp.access_key_id = placeholder_value
    if is_populated(temp.secret_access_key):
        secure_info_found = True
        temp.secret_access_key = placeholder_value
    if is_populated(temp.session_token):
        secure_info_found = True
        temp.session_token = placeholder_value

    return temp if secure_info_found else info
