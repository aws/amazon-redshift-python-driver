import pytest

from redshift_connector import Cursor


@pytest.fixture
def cursor():
    """Create a bare Cursor instance for testing __sanitize_str."""
    return Cursor.__new__(Cursor)


class TestSanitizeStr:
    """Tests for the __sanitize_str character blocklist."""

    @staticmethod
    def _sanitize(cursor, s):
        return cursor._Cursor__sanitize_str(s)

    # ------------------------------------------------------------------
    # Existing blocklist characters (regression tests)
    # ------------------------------------------------------------------

    def test_strips_semicolon(self, cursor):
        assert self._sanitize(cursor, "table;name") == "tablename"

    def test_strips_single_quote(self, cursor):
        assert self._sanitize(cursor, "table'name") == "tablename"

    def test_strips_double_quote(self, cursor):
        assert self._sanitize(cursor, 'table"name') == "tablename"

    def test_strips_hyphen(self, cursor):
        assert self._sanitize(cursor, "table-name") == "tablename"

    def test_strips_forward_slash(self, cursor):
        assert self._sanitize(cursor, "table/name") == "tablename"

    def test_strips_newline(self, cursor):
        assert self._sanitize(cursor, "table\nname") == "tablename"

    def test_strips_carriage_return(self, cursor):
        assert self._sanitize(cursor, "table\rname") == "tablename"

    def test_strips_space(self, cursor):
        assert self._sanitize(cursor, "table name") == "tablename"

    def test_strips_all_existing_blocklist_chars(self, cursor):
        """All original blocklist characters removed in a single string."""
        result = self._sanitize(cursor, "-;/'\"\n\r mixed")
        assert result == "mixed"

    # ------------------------------------------------------------------
    # New blocklist characters added for the \t bypass fix
    # ------------------------------------------------------------------

    def test_strips_tab(self, cursor):
        """Tab was the primary character reported in the bypass."""
        assert self._sanitize(cursor, "table\tname") == "tablename"

    def test_strips_form_feed(self, cursor):
        assert self._sanitize(cursor, "table\fname") == "tablename"

    def test_strips_vertical_tab(self, cursor):
        assert self._sanitize(cursor, "table\vname") == "tablename"

    def test_tab_bypass_poc_payload(self, cursor):
        """The researcher's PoC payload should have all tabs stripped."""
        payload = (
            "exfil_table\tSELECT\tusername,password\tFROM\tsecret_table"
            "\tUNION\tALL\tSELECT\t$$x$$,$$y$$\tWHERE\t1=2"
        )
        result = self._sanitize(cursor, payload)
        assert "\t" not in result
        # After stripping tabs, the tokens collapse into one string.
        # Dollar signs are not in the blocklist, so $$...$$ survives.
        assert result == (
            "exfil_tableSELECTusername,passwordFROMsecret_table"
            "UNIONALLSELECT$$x$$,$$y$$WHERE1=2"
        )

    def test_mixed_new_whitespace_characters(self, cursor):
        """All three new whitespace characters stripped together."""
        result = self._sanitize(cursor, "a\tb\fc\vd")
        assert result == "abcd"

    def test_all_blocklist_chars_combined(self, cursor):
        """Every blocklisted character in one string."""
        result = self._sanitize(cursor, "-;\t/'\"\n\r\f\v name")
        assert result == "name"

    # ------------------------------------------------------------------
    # Valid identifiers pass through unchanged
    # ------------------------------------------------------------------

    def test_simple_identifier_unchanged(self, cursor):
        assert self._sanitize(cursor, "my_table_123") == "my_table_123"

    def test_dotted_identifier_unchanged(self, cursor):
        assert self._sanitize(cursor, "my_schema.my_table") == "my_schema.my_table"

    def test_uppercase_identifier_unchanged(self, cursor):
        assert self._sanitize(cursor, "MY_TABLE") == "MY_TABLE"

    def test_unicode_identifier_unchanged(self, cursor):
        """Non-ASCII letters are not in the blocklist and should pass through."""
        assert self._sanitize(cursor, "用户表") == "用户表"

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_empty_string(self, cursor):
        assert self._sanitize(cursor, "") == ""

    def test_only_blocklisted_characters(self, cursor):
        result = self._sanitize(cursor, "\t\f\v -;/'\"\n\r ")
        assert result == ""

    def test_single_tab_only(self, cursor):
        assert self._sanitize(cursor, "\t") == ""

    def test_consecutive_tabs(self, cursor):
        assert self._sanitize(cursor, "a\t\t\tb") == "ab"

    def test_dollar_sign_not_stripped(self, cursor):
        """Dollar signs are NOT in the blocklist (known gap, out of scope)."""
        result = self._sanitize(cursor, "table$name")
        assert "$" in result

    # ------------------------------------------------------------------
    # Unicode whitespace (covered by \s, not by the old explicit list)
    # ------------------------------------------------------------------

    def test_strips_non_breaking_space(self, cursor):
        """\\xa0 NBSP is a Unicode whitespace matched by \\s."""
        assert self._sanitize(cursor, "table\xa0name") == "tablename"

    def test_strips_next_line(self, cursor):
        """\\x85 NEL is a Unicode whitespace matched by \\s."""
        assert self._sanitize(cursor, "table\x85name") == "tablename"

    def test_strips_em_space(self, cursor):
        """U+2003 EM SPACE is a Unicode whitespace matched by \\s."""
        assert self._sanitize(cursor, "table\u2003name") == "tablename"

    def test_strips_ideographic_space(self, cursor):
        """U+3000 IDEOGRAPHIC SPACE is a Unicode whitespace matched by \\s."""
        assert self._sanitize(cursor, "table\u3000name") == "tablename"
