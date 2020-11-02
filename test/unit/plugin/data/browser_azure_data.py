code: str = "helloworld"
state: str = "abcdefghij"
valid_response: bytes = (
    b"POST /redshift/ HTTP/1.1\r\nHost: localhost:51187\r\nConnection: keep-alive\r\nContent-Length: 695\r\n"
    b"Cache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nOrigin: null\r\n"
    b"Content-Type: application/x-www-form-urlencoded\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    b"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\nAccept: text/html,"
    b"application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
    b"application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: navigate\r\n"
    b"Sec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\n"
    b"\r\ncode=" + code.encode("utf-8") + b"&state=" + state.encode("utf-8") + b"&session_state=hooplah"
)

missing_state_response: bytes = (
    b"POST /redshift/ HTTP/1.1\r\nHost: localhost:51187\r\nConnection: keep-alive\r\nContent-Length: 695\r\n"
    b"Cache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nOrigin: null\r\n"
    b"Content-Type: application/x-www-form-urlencoded\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    b"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\nAccept: text/html,"
    b"application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
    b"application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: navigate\r\n"
    b"Sec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\n"
    b"\r\ncode=" + code.encode("utf-8")
)

mismatched_state_response: bytes = (
    b"POST /redshift/ HTTP/1.1\r\nHost: localhost:51187\r\nConnection: keep-alive\r\nContent-Length: 695\r\n"
    b"Cache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nOrigin: null\r\n"
    b"Content-Type: application/x-www-form-urlencoded\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    b"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\nAccept: text/html,"
    b"application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
    b"application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: navigate\r\n"
    b"Sec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\n"
    b"\r\ncode=" + code.encode("utf-8") + b"&state=" + state[::-1].encode("utf-8") + b"&session_state=hooplah"
)

missing_code_response: bytes = (
    b"POST /redshift/ HTTP/1.1\r\nHost: localhost:51187\r\nConnection: keep-alive\r\nContent-Length: 695\r\n"
    b"Cache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nOrigin: null\r\n"
    b"Content-Type: application/x-www-form-urlencoded\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    b"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\nAccept: text/html,"
    b"application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
    b"application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: navigate\r\n"
    b"Sec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\n"
    b"&state=" + state.encode("utf-8") + b"&session_state=hooplah"
)

empty_code_response: bytes = (
    b"POST /redshift/ HTTP/1.1\r\nHost: localhost:51187\r\nConnection: keep-alive\r\nContent-Length: 695\r\n"
    b"Cache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nOrigin: null\r\n"
    b"Content-Type: application/x-www-form-urlencoded\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    b"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\nAccept: text/html,"
    b"application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
    b"application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: cross-site\r\nSec-Fetch-Mode: navigate\r\n"
    b"Sec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\n"
    b"\r\ncode=" + b"&state=" + state.encode("utf-8") + b"&session_state=hooplah"
)


saml_response = b"my_access_token"

valid_json_response: dict = {
    "token_type": "Bearer",
    "expires_in": "3599",
    "ext_expires_in": "3599",
    "expires_on": "1602782647",
    "resource": "spn:1234567891011121314151617181920",
    "access_token": "bXlfYWNjZXNzX3Rva2Vu",  # base64.urlsafe_64encode(saml_response)
    "issued_token_type": "urn:ietf:params:oauth:token-type:saml2",
    "refresh_token": "my_refresh_token",
    "id_token": "my_id_token",
}

json_response_no_access_token: dict = {
    "token_type": "Bearer",
    "expires_in": "3599",
    "ext_expires_in": "3599",
    "expires_on": "1602782647",
    "resource": "spn:1234567891011121314151617181920",
    "issued_token_type": "urn:ietf:params:oauth:token-type:saml2",
    "refresh_token": "my_refresh_token",
    "id_token": "my_id_token",
}

json_response_empty_access_token: dict = {
    "token_type": "Bearer",
    "expires_in": "3599",
    "ext_expires_in": "3599",
    "expires_on": "1602782647",
    "resource": "spn:1234567891011121314151617181920",
    "access_token": "",
    "issued_token_type": "urn:ietf:params:oauth:token-type:saml2",
    "refresh_token": "my_refresh_token",
    "id_token": "my_id_token",
}
