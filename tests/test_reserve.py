import socket

from utils import receive_data


def test_reserve_after(client: socket.socket) -> None:
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING foo\r\n'))

    message = b'put 500 0 10 53\r\n'
    client.sendall(message)

    job_body = b'01234567890123456789\r\n'
    client.sendall(job_body)
    receive_data(client, len(b'INSERTED X\r\n'))

    use_message = b'watch foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client.sendall(message)

    expected = b'RESERVED 1 53\r\n01234567890123456789\r\n'
    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected
