from unittest.mock import MagicMock

from packaging.version import Version
from redshift_connector.idp_auth_helper import IdpAuthHelper


def test_get_pkg_version(mocker) -> None:
    mocker.patch("importlib.metadata.version", return_value=None)

    module_mock = MagicMock()
    module_mock.__version__ = "9.8.7"
    mocker.patch("importlib.import_module", return_value=module_mock)

    actual_version: Version = IdpAuthHelper.get_pkg_version("test_module")

    assert actual_version == Version("9.8.7")
