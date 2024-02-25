import socket

from utils import receive_data


def test_put_large(client: socket.socket) -> None:
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'using foo\r\n'))

    expected = b'INSERTED 1\r\n'
    message = b'put 500 0 10 53\r\n'
    client.sendall(message)

    job_body = b'01234567890123456789\r\n'
    client.sendall(job_body)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected


def test_put_too_big(client: socket.socket) -> None:
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'using foo\r\n'))

    expected = b'JOB_TOO_BIG\r\n'
    message = b'put 500 0 10 20\r\n'
    client.sendall(message)

    job_body = b'01234567890123456789\r\n'
    client.sendall(job_body)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected


def test_put_without_crlf(client: socket.socket) -> None:
    # TODO: need to check if left_over is the size of the put job
    # TODO: after getting put command line
    # TODO: Beanstalkd has two errors for invalid messages. One is the message must end in \r\n and the second one is job too big. But how do you know when to do one or the other? https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L166
    # TODO: If the message contains everything except for \r\n how do you know you just haven't received \r\n yet?
    use_message = b'use foo\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'using foo\r\n'))

    expected = b'EXPECTED_CRLF\r\n'
    message = b'put 500 0 10 53\r\n01234567890123456789'
    client.sendall(message)

    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected
