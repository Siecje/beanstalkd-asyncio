import asyncio
import logging
import re
import socket
import sys

from protocol import (
    add_job,
    Client,
    drop_connection,
    ensure_tube_has_client,
    ensure_tube_without_client,
    ignore_message,
    Job,
    MAX_JOB_SIZE,
    QuitMessage,
    try_to_issue_job,
    try_to_issue_job_to_client,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

put_head_re = re.compile('put ([0-9]+) ([0-9]+) ([0-9]+) ([0-9]+)')

count_job = 0
in_event_loop = set()


async def issue_job(job: Job) -> None:
    loop = asyncio.get_event_loop()
    job_size = len(job.body)
    message = f'RESERVED {job.id} {job_size}\r\n'
    to_send = message.encode('utf-8') + job.body + b'\r\n'
    await loop.sock_sendall(job.client.connection, to_send)


def handle_message(client: Client, message: bytes) -> str | None:
    if message == b'quit':
        raise QuitMessage
    if message.startswith(b'use '):
        tube = message.replace(b'use ', b'').decode('utf-8')
        client.using = tube
        return f'USING {tube}\r\n'
    if message.startswith(b'watch '):
        tube = message.replace(b'watch ', b'').decode('utf-8')
        client.watching.append(tube)
        ensure_tube_has_client(tube, client)
        count = len(client.watching)
        return f'WATCHING {count}\r\n'
    if message.startswith(b'ignore '):
        tube = message.replace(b'ignore ', b'').decode('utf-8')
        client.watching.remove(tube)
        ensure_tube_without_client(tube, client)
        count = len(client.watching)
        return f'WATCHING {count}\r\n'
    if message.startswith(b'reserve'):
        if not client.watching:
            msg = 'Error: `reserve` without watching a tube.'
            return msg
        task = asyncio.create_task(try_to_issue_job_to_client(client, issue_job))
        in_event_loop.add(task)
        task.add_done_callback(lambda _: in_event_loop.discard(task))
        return
    if message.startswith(b'put '):
        head, body = message.split(b'\r\n')
        (pri, delay, ttr, num_bytes) = put_head_re.match(head.decode('utf-8')).groups()
        tube = client.using
        if tube is None:
            msg = 'Error: `put` without using a tube.'
            return msg
        job = Job(body)
        logger.debug('Job created.')

        job_id = add_job(tube, job)
        task = asyncio.create_task(try_to_issue_job(tube, issue_job))
        in_event_loop.add(task)
        task.add_done_callback(lambda _: in_event_loop.discard(task))
        return f'INSERTED {job_id}\r\n'


async def on_connection(client: Client) -> None:
    message = b''
    try:
        loop = asyncio.get_event_loop()
        while True:
            # TODO: why 100?
            data = await loop.sock_recv(client.connection, 100)
            if data == b'':
                message = b''
                drop_connection(client)
                return

            message += data
            if ignore_message(message):
                message = b''
                continue
            if message.startswith(b'put') and b'\r\n' in message:
                try:
                    (pri, delay, ttr, num_bytes) = put_head_re.match(message.decode('utf-8')).groups()
                except AttributeError:
                    continue
                num_bytes_int = int(num_bytes)
                if num_bytes_int > MAX_JOB_SIZE:
                    # clear message before yielding to event loop
                    message = b''
                    data = await loop.sock_sendall(client.connection, b'JOB_TOO_BIG\r\n')
                    continue
                body = message.split(b'\r\n')[-1]
                if body and len(body) > num_bytes_int:
                    # clear message before yielding to event loop
                    message = b''
                    data = await loop.sock_sendall(client.connection, b'EXPECTED_CRLF\r\n')
                    continue
                if message.count(b'\r\n') == 1:
                    continue
            elif b'\r\n' not in message:
                continue
    
            # message will contain at least one message
            messages = message.split(b'\r\n')
            # Combine put which has two \r\n
            complete_messages = []
            found_put = False
            for idx, message in enumerate(messages):
                if found_put:
                    complete_messages[-1] += b'\r\n' + message
                    found_put = False
                if b'put' in message:
                    found_put = True
                complete_messages.append(message)

            for message in complete_messages:
                try:
                    reply = handle_message(client, message)
                except QuitMessage:
                    drop_connection(client)
                    return
                if reply:
                    await loop.sock_sendall(client.connection, reply.encode('utf-8'))
    finally:
        logger.debug('Connection closed')
        client.connection.close()


def on_done_connection_data(task: asyncio.Task) -> None:
    # Show traceback if there is an exception
    # and remove task from in_event_loop
    try:
        task.result()
    finally:
        in_event_loop.discard(task)


async def on_new_connection(sock: socket.socket) -> None:
    loop = asyncio.get_event_loop()
    while True:
        connection, address = await loop.sock_accept(sock)
        new_client = Client(connection, address)
        logger.debug('New Client created.')
        task = asyncio.create_task(on_connection(new_client))
        # Add reference to task so it isn't garbage collected
        in_event_loop.add(task)
        task.add_done_callback(on_done_connection_data)


def main() -> None:
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_address = ('localhost', 10_000)
    sock.bind(server_address)
    sock.listen(10)
    sock.setblocking(False)

    asyncio.run(on_new_connection(sock))


if __name__ == '__main__':
    main()
