from unittest.mock import MagicMock

import pytest
from packaging.version import Version

from redshift_connector.error import InterfaceError
from redshift_connector.idp_auth_helper import (
    _PLUGIN_ALLOWLIST_ENV_VAR,
    IdpAuthHelper,
    dynamic_plugin_import,
    set_plugin_allowlist,
)


def test_get_pkg_version(mocker) -> None:
    mocker.patch("importlib.metadata.version", return_value=None)

    module_mock = MagicMock()
    module_mock.__version__ = "9.8.7"
    mocker.patch("importlib.import_module", return_value=module_mock)

    actual_version: Version = IdpAuthHelper.get_pkg_version("test_module")

    assert actual_version == Version("9.8.7")


class TestDynamicPluginImportAllowlist:
    """Tests for opt-in plugin allowlist to prevent RCE."""

    def teardown_method(self):
        set_plugin_allowlist(None)

    def test_no_allowlist_allows_any_module(self):
        """With no allowlist configured (default), any module can load."""
        set_plugin_allowlist(None)
        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider

    def test_allowlist_rejects_unlisted_module(self):
        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
            dynamic_plugin_import("os")

    def test_allowlist_rejects_arbitrary_dotted_path(self):
        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
            dynamic_plugin_import("subprocess.Popen")

    def test_allowlist_permits_listed_module(self):
        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider

    def test_empty_allowlist_blocks_everything(self):
        set_plugin_allowlist(set())
        with pytest.raises(InterfaceError):
            dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")

    def test_env_var_allowlist(self, monkeypatch):
        """Environment variable sets allowlist without code changes."""
        set_plugin_allowlist(None)
        monkeypatch.setenv(
            _PLUGIN_ALLOWLIST_ENV_VAR,
            "redshift_connector.plugin.OktaCredentialsProvider,redshift_connector.plugin.PingCredentialsProvider",
        )
        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider

        with pytest.raises(InterfaceError):
            dynamic_plugin_import("os")

    def test_env_var_empty_string_blocks_everything(self, monkeypatch):
        """An empty-string env var rejects every plugin import.

        An empty string means the allowlist is set but has no entries, so
        nothing is allowed.
        """
        set_plugin_allowlist(None)
        monkeypatch.setenv(_PLUGIN_ALLOWLIST_ENV_VAR, "")

        with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
            dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
            dynamic_plugin_import("os")

    def test_env_var_whitespace_only_blocks_everything(self, monkeypatch):
        """Whitespace-only or comma-only env var values reject every plugin.

        These parse down to an empty allowlist, same as an empty string.
        """
        set_plugin_allowlist(None)
        for value in (" ", ",", " , ,"):
            monkeypatch.setenv(_PLUGIN_ALLOWLIST_ENV_VAR, value)
            with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
                dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")

    def test_env_var_unset_allows_any_module(self, monkeypatch):
        """An absent env var leaves no allowlist, so any module loads."""
        set_plugin_allowlist(None)
        monkeypatch.delenv(_PLUGIN_ALLOWLIST_ENV_VAR, raising=False)

        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider

    def test_programmatic_allowlist_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv(_PLUGIN_ALLOWLIST_ENV_VAR, "redshift_connector.plugin.PingCredentialsProvider")
        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        # Programmatic setting wins
        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider

        with pytest.raises(InterfaceError):
            dynamic_plugin_import("redshift_connector.plugin.PingCredentialsProvider")

    def test_load_credentials_provider_rejects_when_allowlist_set(self):
        from redshift_connector.redshift_property import RedshiftProperty

        set_plugin_allowlist({"redshift_connector.plugin.PingCredentialsProvider"})
        info = RedshiftProperty()
        info.credentials_provider = "os.system"
        with pytest.raises(InterfaceError):
            IdpAuthHelper.load_credentials_provider(info)

    def test_load_credentials_provider_accepts_short_name_in_allowlist(self):
        """A short plugin name expands to its full path before the check."""
        from redshift_connector.plugin import OktaCredentialsProvider
        from redshift_connector.redshift_property import RedshiftProperty

        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        info = RedshiftProperty()
        info.credentials_provider = "OktaCredentialsProvider"
        info.idp_host = "test.okta.com"
        info.app_id = "test_app_id"
        provider = IdpAuthHelper.load_credentials_provider(info)
        assert isinstance(provider, OktaCredentialsProvider)

    def test_allowlist_matching_is_case_sensitive(self):
        """Allowlist entries are matched as case-sensitive strings."""
        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
            dynamic_plugin_import("redshift_connector.plugin.oktacredentialsprovider")

    def test_rejection_message_names_the_offending_module(self):
        """The rejection error mentions the exact name that was rejected."""
        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        with pytest.raises(InterfaceError) as excinfo:
            dynamic_plugin_import("subprocess.Popen")
        assert "subprocess.Popen" in str(excinfo.value)

    def test_env_var_deduplicates_repeated_entries(self, monkeypatch):
        """Duplicate env var entries collapse into one allowlist member."""
        set_plugin_allowlist(None)
        monkeypatch.setenv(
            _PLUGIN_ALLOWLIST_ENV_VAR,
            "redshift_connector.plugin.OktaCredentialsProvider," "redshift_connector.plugin.OktaCredentialsProvider",
        )
        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider

    def test_set_plugin_allowlist_rejects_bare_string(self):
        """A plain string is rejected to prevent a substring-match bypass.

        Strings are iterable, so a naive ``name in some_string`` check
        would silently degrade into a substring match, letting short names
        like ``"redshift_connector"`` slip through and defeat the
        allowlist.
        """
        with pytest.raises(TypeError, match="must be None or a set of module path strings"):
            set_plugin_allowlist("redshift_connector.plugin.OktaCredentialsProvider")

    def test_set_plugin_allowlist_rejects_non_string_members(self):
        """Every member of the allowlist must be a string."""
        with pytest.raises(TypeError, match="must be None or a set of module path strings"):
            set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider", 123})

    def test_set_plugin_allowlist_accepts_iterable_of_strings(self):
        """A list or tuple of strings is accepted and normalized to a set."""
        set_plugin_allowlist(["redshift_connector.plugin.OktaCredentialsProvider"])
        klass = dynamic_plugin_import("redshift_connector.plugin.OktaCredentialsProvider")
        from redshift_connector.plugin import OktaCredentialsProvider

        assert klass is OktaCredentialsProvider
        # Any name outside the allowlist is still rejected, proving the stored
        # value behaves as a set (not the raw iterable).
        with pytest.raises(InterfaceError, match="not in the configured plugin allowlist"):
            dynamic_plugin_import("os")

    def test_load_credentials_provider_preserves_allowlist_reason_in_traceback(self):
        """The wrapper error keeps the original allowlist reason as its cause.

        The user-facing message stays generic, but the specific "not in
        the allowlist" reason still shows up in the traceback via
        ``__cause__``, which helps debugging.
        """
        from redshift_connector.redshift_property import RedshiftProperty

        set_plugin_allowlist({"redshift_connector.plugin.OktaCredentialsProvider"})
        info = RedshiftProperty()
        info.credentials_provider = "os.system"
        with pytest.raises(InterfaceError) as excinfo:
            IdpAuthHelper.load_credentials_provider(info)
        assert excinfo.value.__cause__ is not None
        assert "not in the configured plugin allowlist" in str(excinfo.value.__cause__)
