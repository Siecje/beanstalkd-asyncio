import socket

from utils import receive_data


def test_watch_ignore(client: socket.socket) -> None:
    expected = b'WATCHING 1\r\n'
    message = b'watch foo\r\n'
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected

    expected = b'WATCHING 2\r\n'
    message = b'watch bar\r\n'
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected

    expected = b'WATCHING 1\r\n'
    message = b'ignore foo\r\n'
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected

    expected = b'WATCHING 0\r\n'
    message = b'ignore bar\r\n'
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected
