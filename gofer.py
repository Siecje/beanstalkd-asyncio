import asyncio
import logging
import re
import socket
import sys


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

put_head_re = re.compile('put ([0-9]+) ([0-9]+) ([0-9]+) ([0-9]+)')

count_job = 0
in_event_loop = set()
tubes = {}


class Client:
    def __init__(self, connection: socket.socket, address: tuple[str, int]) -> None:
        self.address = address
        self.connection = connection
        self.job = None
        self.using = None
        self.watching = []


class Job:
    def __init__(self, body: str) -> None:
        self.client = None
        self.body = body
        self.id = None


def issue_job(job: Job) -> None:
    async def issue_client_job(job: Job) -> None:
        loop = asyncio.get_event_loop()
        job_size = sys.getsizeof(job.body)
        message = f'RESERVED {job.id} {job_size}\r\n'
        to_send = message.encode('utf-8') + job.body + b'\r\n'
        await loop.sock_sendall(job.client.connection, to_send)

    task = asyncio.create_task(issue_client_job(job))
    in_event_loop.add(task)
    task.add_done_callback(lambda _: in_event_loop.discard(task))


def try_to_issue_job(tube: str) -> bool:
    job = None
    for j in tubes[tube]['jobs'].values():
        if j.client is None:
            job = j

    if job is None:
        return False

    client = None
    for c in tubes[tube]['clients']:
        if c.job is None:
            client = c

    if client is None:
        return False

    try:
        job.client = client
        client.job = job

        issue_job(job)
    except Exception:
        job.client = None
        client.job = None
        return False
    else:
        return True


def try_to_issue_job_to_client(client: Client) -> None:
    for tube in client.watching:
        if try_to_issue_job(tube):
            break


def add_job(tube: str, job: Job) -> int:
    global count_job
    count_job += 1
    job.id = count_job
    if tube in tubes:
        tubes[tube]['jobs'][count_job] = job
    else:
        tubes[tube] = {'clients': [], 'jobs': {count_job: job}}

    return count_job


def ensure_tube_has_client(tube: str, client: Client) -> None:
    if tube not in tubes:
        tubes[tube] = {'clients': [client], 'jobs': {}}
    else:
        current_clients = tubes[tube]['clients']
        clients_without_new_address = [
            c for c in current_clients if c.address != client.address
        ]
        tubes[tube]['clients'] = [*clients_without_new_address, client]


def ensure_tube_without_client(tube: str, client: Client) -> None:
    if tube not in tubes:
        return
    
    current_clients = tubes[tube]['clients']
    clients_without_new_address = [
        c for c in current_clients if c.address != client.address
    ]
    tubes[tube]['clients'] = clients_without_new_address


class BadMessage(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


class QuitMessage(Exception):
    pass


def handle_message(client: Client, message: bytes) -> str | None:
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
        # TODO: this could be async
        try_to_issue_job_to_client(client)
        return
    if message.startswith(b'put '):
        head, body, _ = message.split(b'\r\n')
        (pri, delay, ttr, num_bytes) = put_head_re.match(head.decode('utf-8')).groups()
        if sys.getsizeof(body) != int(num_bytes):
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
        # TODO: this could be async
        try_to_issue_job(tube)
        return f'INSERTED {job_id}\r\n'


def on_drop_connection(client: Client) -> None:
    if client.watching:
        # Remove client from tube
        tubes[client.watching]['clients'] = [
            c for c in tubes[client.watching]['clients']
            if c.address != client.address
        ]
    if client.job:
        client.job.client = None
        client.job = None


async def on_connection_data(client: Client) -> None:
    left_over = b''
    try:
        loop = asyncio.get_event_loop()
        while True:
            # TODO: why 100?
            data = await loop.sock_recv(client.connection, 100)
            if data == b'':
                on_drop_connection(client)
                return
            logger.debug(data)
            if left_over:
                data = left_over + data
                left_over = b''
            messages = data.split(b'\r\n')
            if not data.endswith(b'\r\n'):
                left_over = messages.pop()

            # Combine put which has two \r\n
            valid_messages = []
            temp = b''
            for idx, message in enumerate(messages):
                if b'put' in message:
                    # combine this message with the next message
                    # if this is the last message prepend to left_over
                    if idx == len(messages):
                        left_over = message + left_over
                        # remove from messages
                    else:
                        # combine with next message
                        temp = message + b'\r\n'
                elif temp:
                    valid_messages.append(temp + message + b'\r\n')
                    temp = b''
                else:
                    valid_messages.append(message)

            logger.debug('valid_messages')
            logger.debug(valid_messages)
            for message in valid_messages:
                try:
                    reply = handle_message(client, message)
                except QuitMessage:
                    logger.debug('quit message')
                    on_drop_connection(client)
                    return
                if reply:
                    logger.debug(reply)
                    await loop.sock_sendall(client.connection, reply.encode('utf-8'))
                    logger.debug('after send reply')
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
        task = asyncio.create_task(on_connection_data(new_client))
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
