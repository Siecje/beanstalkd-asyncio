import functools
import logging
import re
import sys

import trio

from protocol import (
    add_job,
    BadMessage,
    Client,
    drop_connection,
    ensure_tube_has_client,
    ensure_tube_without_client,
    get_job_with_client,
    ignore_message,
    Job,
    QuitMessage,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

put_head_re = re.compile('put ([0-9]+) ([0-9]+) ([0-9]+) ([0-9]+)')

MAX_JOB_SIZE = 2 ** 16

async def issue_job(job: Job) -> None:
    job_size = len(job.body)
    message = f'RESERVED {job.id} {job_size}\r\n'
    to_send = message.encode('utf-8') + job.body + b'\r\n'
    await job.client.connection.send_all(to_send)


async def try_to_issue_job(nursery, tube: str) -> bool:
    job = get_job_with_client(tube)
    if job:
        nursery.start_soon(issue_job, job)
        return True
    return False


async def try_to_issue_job_to_client(nursery, client: Client) -> None:
    for tube in client.watching:
        success = await try_to_issue_job(nursery, tube)
        if success:
            break


def handle_message(nursery, client: Client, message: bytes) -> str | None:
    logger.debug('handle_message() %s', message)
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
        nursery.start_soon(try_to_issue_job_to_client, nursery, client)
        return
    if message.startswith(b'put '):
        head, body = message.split(b'\r\n')
        (pri, delay, ttr, num_bytes) = put_head_re.match(head.decode('utf-8')).groups()
        if int(num_bytes) > 2 ** 16:
            logger.debug(sys.getsizeof(body))
            msg = "JOB_TOO_BIG\r\n"
            return msg
        tube = client.using
        if tube is None:
            msg = 'Error: `put` without using a tube.'
            return msg
        job = Job(body)
        logger.debug('Job created.')

        job_id = add_job(tube, job)
        nursery.start_soon(try_to_issue_job, nursery, tube)
        return f'INSERTED {job_id}\r\n'


async def server(connection, nursery) -> None:
    address = connection.socket.getsockname()
    client = Client(connection, address)
    
    message = b''
    async for data in connection:
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
                await connection.send_all(b'JOB_TOO_BIG\r\n')
                continue
            body = message.split(b'\r\n')[-1]
            if body and len(body) > num_bytes_int:
                # clear message before yielding to event loop
                message = b''
                await connection.send_all(b'EXPECTED_CRLF\r\n')
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
                reply = handle_message(nursery, client, message)
            except QuitMessage:
                drop_connection(client)
                return
            if reply:
                await connection.send_all(reply.encode('utf-8'))


async def main() -> None:
    async with trio.open_nursery() as nursery:
        server_task = functools.partial(server, nursery=nursery)
        nursery.start_soon(trio.serve_tcp, server_task, 10_000)


if __name__ == '__main__':
    trio.run(main)
