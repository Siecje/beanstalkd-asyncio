import heapq

import trio


tubes = {}
count_job = 0

MAX_JOB_SIZE = 2 ** 16


class Client:
    def __init__(self, connection, address) -> None:
        self.address = address
        self.connection = connection
        self.job = None
        self.using = None
        self.watching = []


class Job:
    def __init__(self, body: str, priority: int, ttr: int) -> None:
        self.client = None
        self.body = body
        self.id = None
        self.priority = priority
        self.state = 'ready'
        self.ttr = ttr

    def __lt__(self, other):
        return self.priority < other.priority


def delete_job(job: Job) -> None:
    job_id = job.id
    print('job_id', job_id)
    for tube in tubes:
        for idx, (_, j) in enumerate(tubes[tube]['jobs']):
            if j.id == job_id:
                try:
                    j.client.job = None
                except AttributeError:
                    # tubes[tube]['jobs'][job_id].client is None
                    pass
                j.client = None
                del j
                del tubes[tube]['jobs'][idx]
                return


def get_job_by_id(job_id: int) -> Job | None:
    for tube in tubes:
        for _, job in tubes[tube]['jobs']:
            if job.id == job_id:
                return job


def get_job_with_client(tube: str) -> Job | None:
    job = None
    for _, j in tubes[tube]['jobs']:
        if j.state == 'ready':
            job = j

    if job is None:
        return None

    client = None
    for c in tubes[tube]['clients']:
        if c.job is None:
            client = c

    if client is None:
        return None

    try:
        job.client = client
        client.job = job
    except Exception:
        job.client = None
        client.job = None
        return None
    else:
        return job


def ensure_tube_has_client(tube: str, client: Client) -> None:
    if tube not in tubes:
        tubes[tube] = {'clients': [client], 'jobs': []}
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


class QuitMessage(Exception):
    pass


def drop_connection(client: Client) -> None:
    # Remove client from tubes
    for tube in client.watching:
        tubes[tube]['clients'] = [
            c for c in tubes[tube]['clients']
            if c.address != client.address
        ]
    if client.job:
        client.job.client = None
        client.job = None


def add_job(tube: str, job: Job) -> int:
    global count_job
    count_job += 1
    job.id = count_job
    if tube not in tubes:
        tubes[tube] = {'clients': [], 'jobs': []}
    heapq.heappush(tubes[tube]['jobs'], (job.priority, job))
    return job.id


def ignore_message(message):
    if len(message) < 3:
        return False
    # Get the message until the first space
    # If valid message_start will be the command or the first part of a command
    message_start = message.strip()[:message.find(b' ')]

    # Ensure that the message starts with one of the commands
    commands = (
        b'bury',
        b'delete',
        b'ignore',
        b'kick',
        b'kick-job',
        b'list-tubes',
        b'list-tubes-used',
        b'list-tubes-watched',
        b'pause-tube',
        b'peek',
        b'peek-buried',
        b'peek-delayed',
        b'peek-ready',
        b'put',
        b'quit',
        b'release',
        b'reserve',
        b'reserve-job',
        b'reserve-with-timeout',
        b'stats-job',
        b'stats-tube',
        b'stats',
        b'touch',
        b'use',
        b'watch',
    )
    for command in commands:
        if command.startswith(message_start):
            return False
    else:
        return True


async def try_to_issue_job(tube: str, issue_job) -> Client | None:
    job = get_job_with_client(tube)
    if not job:
        return None

    client = job.client
    success = await issue_job(job)
    if success:
        job.state = 'reserved'
        return client
    else:
        try:
            job.client.job = None
        except AttributeError:
            pass
        job.client = None
        job.state = 'ready'
        return None


async def try_to_issue_job_to_client(client: Client, issue_job, job_given: trio.Event = None) -> None:
    for tube in client.watching:
        job_client = await try_to_issue_job(tube, issue_job)
        if isinstance(job_client, Client) and job_client == client:
            if job_given:
                job_given.set()
            break


def release_job(job: Job):
    try:
        job.client.job = None
    except AttributeError:
        pass
    job.client = None
