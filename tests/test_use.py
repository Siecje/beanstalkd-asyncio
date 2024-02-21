import socket

from utils import receive_data


def test_use(client: socket.socket) -> None:
    expected = b'USING foo\r\n'
    message = b'use foo\r\n'
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected
