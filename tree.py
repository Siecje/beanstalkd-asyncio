import functools
import logging
import re
import sys

import trio

from protocol import (
    add_job,
    Client,
    delete_job,
    drop_connection,
    ensure_tube_has_client,
    ensure_tube_without_client,
    get_job_by_id,
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


async def issue_job(job: Job) -> None:
    job_size = len(job.body)
    message = f'RESERVED {job.id} {job_size}\r\n'
    to_send = message.encode('utf-8') + job.body + b'\r\n'
    try:
        await job.client.connection.send_all(to_send)
    except Exception:
        return False
    else:
        return True


async def reserve_with_timeout(client, time_s):
    job_given = trio.Event()
    async with trio.open_nursery() as nursery:
        nursery.start_soon(try_to_issue_job_to_client, client, issue_job, job_given)
        with trio.move_on_after(time_s):
            await job_given.wait()
        nursery.cancel_scope.cancel()
    if not job_given.is_set():
        to_send = b'TIMED_OUT\r\n'
        await client.connection.send_all(to_send)


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
    if message.startswith(b'reserve-with-timeout'):
        if not client.watching:
            msg = 'Error: `reserve` without watching a tube.'
            return msg
        time_s = int(message.replace(b'reserve-with-timeout ', b'').strip())
        nursery.start_soon(reserve_with_timeout, client, time_s)
        return
    if message.startswith(b'reserve'):
        if not client.watching:
            msg = 'Error: `reserve` without watching a tube.'
            return msg
        nursery.start_soon(try_to_issue_job_to_client, client, issue_job)
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
        nursery.start_soon(try_to_issue_job, tube, issue_job)
        return f'INSERTED {job_id}\r\n'
    if message.startswith(b'delete '):
        job_id = int(message.replace(b'delete ', b'').strip())
        job = get_job_by_id(job_id)
        if not job:
            return 'NOT_FOUND\r\n'

        if not job.client or (job.client and client == job.client):
            delete_job(job)
            return 'DELETED\r\n'
        return 'NOT_FOUND\r\n'


async def server(connection, nursery) -> None:
    address = connection.socket.getsockname()
    client = Client(connection, address)
    
    message = b''
    try:
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
    except trio.BrokenResourceError:
        drop_connection(client)
        return


async def main() -> None:
    async with trio.open_nursery() as nursery:
        server_task = functools.partial(server, nursery=nursery)
        nursery.start_soon(trio.serve_tcp, server_task, 10_000)


if __name__ == '__main__':
    trio.run(main)
