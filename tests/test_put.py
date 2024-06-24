import re
import socket
import time

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


def test_put_with_timeout(client, client2):
    watch_message = b'watch test_put_with_timeout\r\n'
    client.sendall(watch_message)
    receive_data(client, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client.sendall(message)

    use_message = b'use test_put_with_timeout\r\n'
    client2.sendall(use_message)
    receive_data(client2, len(b'using test_put_with_timeout\r\n'))

    job_body = b'01234567890123456789'
    job_time = 10
    message = b'put 500 10 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'

    expected = b'RESERVED X ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'
    amount_expected = len(expected)
    
    client2.sendall(message)

    start_time = time.time()
    # Should fail if client waits longer than timeout_s
    timeout_s = 11
    data = receive_data(client, amount_expected, timeout_s)
    end_time = time.time()
    # This times when the full job has been received
    # This doesn't time when the job is first send
    assert (job_time - 1) < (end_time - start_time) < (job_time + 1)
    put_response = receive_data(client2, len(b'INSERTED X\r\n'))
    job_id = put_response.replace(b'INSERTED ', b'').strip()
    expected = expected.replace(b'X', job_id)
    assert data == expected
