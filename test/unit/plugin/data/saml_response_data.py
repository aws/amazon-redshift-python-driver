import base64
import zlib

# uses sample (valid) SAML response from data folder
saml_response: bytes = open("test/unit/plugin/data/saml_response.xml").read().encode("utf-8")

# the SAML response as received in HTTP response
encoded_saml_response: bytes = base64.b64encode(zlib.compress(saml_response)[2:-4])


# HTTP response containing SAML response can be formatted two ways.
# 1. HTTP response containing things other than the SAML response
valid_http_response_with_header_equal_delim = (
    b"POST /redshift/ HTTP/1.1\r\nHost: localhost:7890\r\nConnection: keep-alive\r\nContent-Length: "
    b"11639\r\nCache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nOrigin: null\r\nContent-Type: "
    b"application/x-www-form-urlencoded\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) "
    b"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36\r\nAccept: text/html,"
    b"application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,"
    b"application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: "
    b"navigate\r\nSec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,"
    b"en;q=0.9\r\n\r\nSAMLResponse=" + encoded_saml_response + b"&RelayState="
)

valid_http_response_with_header_colon_delim = b"POST\r\nRelayState: \r\nSAMLResponse:\r\n" + encoded_saml_response

# 2. HTTP response containing *only* the SAML response
valid_http_response_no_header: bytes = b"SAMLResponse=" + encoded_saml_response + b"&RelayState="
