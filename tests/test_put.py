import re
import socket

from utils import receive_data


def test_put(client: socket.socket) -> None:
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'using foo\r\n'))

    message = b'put 500 0 10 53\r\n'
    client.sendall(message)

    job_body = b'01234567890123456789\r\n'
    client.sendall(job_body)

    expected = b'INSERTED X\r\n'
    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert re.match(expected.replace(b'X', b'[0-9]+'), data)


def test_put_job_larger_than_max_job_size(client: socket.socket) -> None:
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'using foo\r\n'))

    expected = b'JOB_TOO_BIG\r\n'
    put_command_line_bytes = (2 ** 16) + 1
    message = b'put 500 0 10 ' + str(put_command_line_bytes).encode('utf-8') + b'\r\n'
    client.sendall(message)

    job_body = b'9' * put_command_line_bytes + b'\r\n'
    client.sendall(job_body)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected


def test_put_without_crlf(client: socket.socket) -> None:
    # job body is larger than <bytes> in put command line
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'using foo\r\n'))

    expected = b'EXPECTED_CRLF\r\n'
    body = b'01234567890123456789'
    body_size = len(body)
    message = b'put 500 0 10 ' + str(body_size - 1).encode('utf-8') + b'\r\n' + body
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected
