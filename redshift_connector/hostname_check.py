import ipaddress
import typing

"""
Hostname matching for sslmode=verify-full.

Owned by the driver because ``ssl.match_hostname`` was removed in 3.12 while
setup.py still supports 3.8, and because the check must run after the handshake
so a mismatch can be a warning. Follows RFC 6125: subjectAltName is
authoritative, Common Name only when no dNSName entry is present.
"""

__all__ = ["match_hostname"]


def _dnsname_match(pattern: str, hostname: str) -> bool:
    """Compare one dNSName pattern against a hostname, honouring a wildcard.

    One wildcard, only as the whole left-most label, never matching a dot. Kept as
    a label comparison rather than a regex, where an unescaped ``.`` silently
    becomes "any character" and a wildcard can span dots.

    :param pattern: one dNSName or Common Name from the certificate. None and
        empty return False, so a caller needs no guard of its own.
    :param hostname: the host the client asked to connect to.
    :return: True only when the certificate name covers the hostname.
    """
    if not pattern or not hostname:
        return False

    # A trailing dot marks an absolute name (RFC 1034), so "example.com." and
    # "example.com" are one host; either side may carry it.
    pattern = pattern.lower().rstrip(".")
    hostname = hostname.lower().rstrip(".")

    if "*" not in pattern:
        return pattern == hostname

    labels = pattern.split(".")

    # Left-most label only, and enough labels left that it cannot cover the domain.
    if labels[0] != "*" or len(labels) < 3:
        return False
    if any("*" in label for label in labels[1:]):
        return False

    host_labels = hostname.split(".")
    if len(host_labels) != len(labels):
        return False

    return host_labels[1:] == labels[1:] and host_labels[0] != ""


def _ipaddress_match(pattern: str, hostname: str) -> bool:
    """Compare a SAN iPAddress entry against a literal address."""
    try:
        return ipaddress.ip_address(pattern.rstrip()) == ipaddress.ip_address(hostname)
    except ValueError:
        return False


def _is_ip_address(hostname: str) -> bool:
    try:
        ipaddress.ip_address(hostname)
        return True
    except ValueError:
        return False


def match_hostname(cert: typing.Optional[typing.Dict[str, typing.Any]], hostname: str) -> bool:
    """Whether ``cert`` names ``hostname``.

    Returns False rather than raising, so the caller decides whether a mismatch
    is fatal.

    :param cert: the dict returned by ``SSLSocket.getpeercert()``
    :param hostname: the host the client asked to connect to
    :return: True when the certificate identifies this host. False when it does
        not, or when there is no certificate or hostname to compare.
    """
    if not cert or not hostname:
        return False

    san: typing.Tuple[typing.Tuple[str, str], ...] = cert.get("subjectAltName", ())
    dns_names: typing.List[str] = []
    ip_entries: typing.List[str] = []

    # "DNS" and "IP Address" are what Python's ssl module puts in getpeercert();
    # the certificate itself carries dNSName and iPAddress GeneralName types.
    for key, value in san:
        if key == "DNS":
            dns_names.append(value)
        elif key == "IP Address":
            ip_entries.append(value)

    if _is_ip_address(hostname):
        # An address literal matches an iPAddress entry only, never a dNSName.
        return any(_ipaddress_match(entry, hostname) for entry in ip_entries)

    if any(_dnsname_match(name, hostname) for name in dns_names):
        return True

    # Common Name is the subject's commonName ("CN=..."), where hostnames lived
    # before subjectAltName. Any SAN at all suppresses it (RFC 6125 6.4.4).
    if san:
        return False

    for attribute_set in cert.get("subject", ()):
        for key, value in attribute_set:
            if key == "commonName" and _dnsname_match(value, hostname):
                return True

    return False
