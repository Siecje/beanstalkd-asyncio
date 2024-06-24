import re
import socket

from utils import receive_data


def test_reserve_after_put(client: socket.socket) -> None:
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING foo\r\n'))

    job_body = b'01234567890123456789'
    message = b'put 500 0 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n'
    client.sendall(message)

    client.sendall(job_body + b'\r\n')
    receive_data(client, len(b'INSERTED X\r\n'))

    watch_message = b'watch foo\r\n'
    client.sendall(watch_message)
    receive_data(client, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client.sendall(message)

    expected = b'RESERVED X ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'
    amount_expected = len(expected)

    data = receive_data(client, amount_expected)
    assert re.match(expected.replace(b'X', b'[0-9]+'), data)


def test_reserve_before_put(client: socket.socket, client2: socket.socket) -> None:
    watch_message = b'watch foo\r\n'
    client.sendall(watch_message)
    receive_data(client, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client.sendall(message)

    use_message = b'use foo\r\n'
    client2.sendall(use_message)
    receive_data(client2, len(b'USING foo\r\n'))

    job_body = b'01234567890123456789'
    message = b'put 500 0 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n'
    client2.sendall(message)

    client2.sendall(job_body + b'\r\n')
    receive_data(client2, len(b'INSERTED X\r\n'))

    expected = b'RESERVED X ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'
    amount_expected = len(expected)

    data = receive_data(client, amount_expected)
    assert re.match(expected.replace(b'X', b'[0-9]+'), data)
