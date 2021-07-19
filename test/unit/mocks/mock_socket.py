import socket


class MockSocket(socket.socket):
    mocked_data = None

    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def setsockopt(self, level, optname, value):
        pass

    def bind(self, address):
        pass

    def listen(self, __backlog=...):
        pass

    def settimeout(self, value):
        pass

    def accept(self):
        return (MockSocket(), "127.0.0.1")

    def recv(self, bufsize, flags=...):
        return self.mocked_data

    def close(self) -> None:
        pass

    def send(self, *args) -> None:  # type: ignore
        pass
