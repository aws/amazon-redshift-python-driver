import typing

import pytest

from redshift_connector.hostname_check import match_hostname


def dns_cert(*names: str) -> typing.Dict[str, typing.Any]:
    """A certificate carrying only dNSName SAN entries."""
    return {"subjectAltName": tuple(("DNS", name) for name in names)}


def ip_cert(*addresses: str) -> typing.Dict[str, typing.Any]:
    return {"subjectAltName": tuple(("IP Address", address) for address in addresses)}


def cn_only_cert(common_name: str) -> typing.Dict[str, typing.Any]:
    """A certificate with no SAN at all, so the Common Name is consulted."""
    return {"subject": ((("commonName", common_name),),)}


@pytest.mark.parametrize(
    "pattern, hostname, expected",
    [
        # Exact names.
        ("example.com", "example.com", True),
        ("example.com", "other.com", False),
        # Comparison is case insensitive and tolerates a trailing dot.
        ("EXAMPLE.com", "example.COM", True),
        ("example.com.", "example.com", True),
        # A wildcard covers exactly one label.
        ("*.example.com", "a.example.com", True),
        ("*.example.com", "example.com", False),
        ("*.example.com", "a.b.example.com", False),
        # Partial wildcards are not honoured: f*.example.com must not match.
        ("f*.example.com", "foo.example.com", False),
        # A wildcard is only meaningful as the left-most label.
        ("a.*.example.com", "a.b.example.com", False),
        # A wildcard must not cover a whole public suffix.
        ("*.com", "example.com", False),
        ("*", "example.com", False),
    ],
)
def test_dnsname_patterns(pattern, hostname, expected) -> None:
    assert match_hostname(dns_cert(pattern), hostname) is expected


def test_matches_any_of_several_sans() -> None:
    cert = dns_cert("other.com", "example.com", "*.test.com")

    assert match_hostname(cert, "example.com") is True
    assert match_hostname(cert, "a.test.com") is True
    assert match_hostname(cert, "nope.com") is False


def test_common_name_used_when_no_san_present() -> None:
    assert match_hostname(cn_only_cert("example.com"), "example.com") is True


def test_common_name_ignored_when_a_dnsname_san_exists() -> None:
    # RFC 6125: once the certificate offers dNSName entries, the Common Name is
    # not a fallback. A cert whose SANs simply do not match must not be rescued
    # by its CN.
    cert = dns_cert("other.com")
    cert.update(cn_only_cert("example.com"))

    assert match_hostname(cert, "example.com") is False


def test_ip_literal_matches_only_an_ip_san() -> None:
    assert match_hostname(ip_cert("127.0.0.1"), "127.0.0.1") is True
    assert match_hostname(ip_cert("10.0.0.1"), "127.0.0.1") is False


def test_ip_literal_is_not_matched_by_a_dnsname() -> None:
    # A dNSName that happens to look like an address does not authenticate an
    # address literal.
    assert match_hostname(dns_cert("127.0.0.1"), "127.0.0.1") is False


def test_ipv6_literal() -> None:
    assert match_hostname(ip_cert("::1"), "::1") is True


def test_any_san_suppresses_the_common_name_fallback() -> None:
    # RFC 6125 section 6.4.4: the Common Name is consulted only when the
    # certificate presents no subjectAltName at all. A certificate carrying
    # only iPAddress entries still has a SAN, so dialling it by name must not
    # reach the CN -- otherwise a SAN-bearing certificate could be rescued by
    # a CN naming someone else.
    cert = {
        "subjectAltName": (("IP Address", "10.0.0.1"),),
        "subject": ((("commonName", "example.com"),),),
    }
    assert match_hostname(cert, "example.com") is False

    # That same CN does authenticate the name once no SAN is present, which is
    # what makes the assertion above about the SAN rather than the CN.
    assert match_hostname(cn_only_cert("example.com"), "example.com") is True


@pytest.mark.parametrize("cert", [None, {}, {"subjectAltName": ()}])
def test_absent_or_empty_certificate_never_matches(cert) -> None:
    assert match_hostname(cert, "example.com") is False


def test_empty_hostname_never_matches() -> None:
    assert match_hostname(dns_cert("example.com"), "") is False
