import re
import socket
import time

from utils import receive_data


def test_delete(
    client: socket.socket,
    client2: socket.socket,
) -> None:
    use_message = b'use pee\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING pee\r\n'))

    job_body = b'01234567890123456789'
    message = b'put 500 0 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n'
    client.sendall(message)

    client.sendall(job_body + b'\r\n')
    receive_data(client, len(b'INSERTED X\r\n'))

    watch_message = b'watch pee\r\n'
    client2.sendall(watch_message)
    receive_data(client2, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client2.sendall(message)

    expected = b'RESERVED X ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'
    amount_expected = len(expected)

    data = receive_data(client2, amount_expected)
    job_id = re.match(expected.replace(b'X', b'([0-9]+)'), data).groups()[0]

    message = b'delete <id>\r\n'
    message = message.replace(b'<id>', job_id)
    client2.sendall(message)

    expected = b'DELETED\r\n'
    amount_expected = len(expected)
    data = receive_data(client2, amount_expected)
    assert data == expected


def test_delete_without_reserve(
    client: socket.socket,
    client2: socket.socket,
) -> None:
    use_message = b'use pee\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING foo\r\n'))

    job_body = b'01234567890123456789'
    message = b'put 500 0 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n'
    client.sendall(message)

    client.sendall(job_body + b'\r\n')
    data = receive_data(client, len(b'INSERTED X\r\n'))
    job_id = data.replace(b'INSERTED ', b'').strip()

    watch_message = b'watch pee\r\n'
    client2.sendall(watch_message)
    receive_data(client2, len(b'WATCHING 1\r\n'))

    message = b'delete <id>\r\n'
    message = message.replace(b'<id>', job_id)
    client2.sendall(message)

    expected = b'DELETED\r\n'
    amount_expected = len(expected)
    data = receive_data(client2, amount_expected)
    assert data == expected


def test_delete_reserve_by_other(
    client: socket.socket,
    client2: socket.socket,
    client3: socket.socket,
):
    use_message = b'use pee\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING pee\r\n'))

    job_body = b'01234567890123456789'
    message = b'put 500 0 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n'
    client.sendall(message)

    client.sendall(job_body + b'\r\n')
    receive_data(client, len(b'INSERTED X\r\n'))

    watch_message = b'watch pee\r\n'
    client2.sendall(watch_message)
    receive_data(client2, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client2.sendall(message)

    expected = b'RESERVED X ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'
    amount_expected = len(expected)

    data = receive_data(client2, amount_expected)
    job_id = re.match(expected.replace(b'X', b'([0-9]+)'), data).groups()[0]

    message = b'delete <id>\r\n'
    message = message.replace(b'<id>', job_id)
    client3.sendall(message)

    expected = b'NOT_FOUND\r\n'
    amount_expected = len(expected)
    data = receive_data(client3, amount_expected)
    assert data == expected


def test_delete_after_ttr(
    client: socket.socket,
    client2: socket.socket,
) -> None:
    # Since no other clients it will be deleted
    # https://github.com/beanstalkd/beanstalkd/issues/653
    # TODO: test with another client waiting for a job
    ttr = 10
    use_message = b'use pee\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING pee\r\n'))

    job_body = b'01234567890123456789'
    job_length = len(job_body)
    message = f'put 500 0 {ttr} {job_length}\r\n'
    client.sendall(message.encode('utf-8'))

    client.sendall(job_body + b'\r\n')
    receive_data(client, len(b'INSERTED X\r\n'))

    watch_message = b'watch pee\r\n'
    client2.sendall(watch_message)
    receive_data(client2, len(b'WATCHING 1\r\n'))

    message = b'reserve\r\n'
    client2.sendall(message)

    expected = b'RESERVED X ' + str(len(job_body)).encode('utf-8') + b'\r\n' + job_body + b'\r\n'
    amount_expected = len(expected)

    data = receive_data(client2, amount_expected)
    job_id = re.match(expected.replace(b'X', b'([0-9]+)'), data).groups()[0]

    time.sleep(ttr)

    message = b'delete <id>\r\n'
    message = message.replace(b'<id>', job_id)
    client2.sendall(message)

    expected = b'DELETED\r\n'
    amount_expected = len(expected)
    data = receive_data(client2, amount_expected)
    assert data == expected


def test_delete_unknown_id(client: socket.socket):
    message = b'delete 999\r\n'
    client.sendall(message)

    expected = b'NOT_FOUND\r\n'
    amount_expected = len(expected)
    data = receive_data(client, amount_expected)
    assert data == expected


def test_delete_without_watching(
    client: socket.socket,
    client2: socket.socket,
):
    # Can delete jobs without watching that tube
    # as long as they are not reserved by someone else
    use_message = b'use pee\r\n'
    client.sendall(use_message)
    receive_data(client, len(b'USING pee\r\n'))

    job_body = b'01234567890123456789'
    message = b'put 500 0 10 ' + str(len(job_body)).encode('utf-8') + b'\r\n'
    client.sendall(message)

    client.sendall(job_body + b'\r\n')
    data = receive_data(client, len(b'INSERTED X\r\n'))
    job_id = data.replace(b'INSERTED ', b'').strip()

    message = b'delete <id>\r\n'
    message = message.replace(b'<id>', job_id)
    client2.sendall(message)

    expected = b'DELETED\r\n'
    amount_expected = len(expected)
    data = receive_data(client2, amount_expected)
    assert data == expected
