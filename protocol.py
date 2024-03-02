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
    def __init__(self, body: str) -> None:
        self.client = None
        self.body = body
        self.id = None


def delete_job(job: Job):
    job_id = job.id
    for tube in tubes:
        if job_id in tubes[tube]['jobs']:
            try:
                tubes[tube]['jobs'][job_id].client.job = None
            except AttributeError:
                # tubes[tube]['jobs'][job_id].client is None
                pass
            tubes[tube]['jobs'][job_id].client = None
            del tubes[tube]['jobs'][job_id]


def get_job_by_id(job_id: int) -> Job | None:
    for tube in tubes:
        if job_id in tubes[tube]['jobs']:
            return tubes[tube]['jobs'][job_id]


def get_job_with_client(tube: str) -> Job | None:
    job = None
    for j in tubes[tube]['jobs'].values():
        if j.client is None:
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
    if tube in tubes:
        tubes[tube]['jobs'][count_job] = job
    else:
        tubes[tube] = {'clients': [], 'jobs': {count_job: job}}

    return count_job


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
    if job:
        client = job.client
        success = await issue_job(job)
        if success:
            return client
        else:
            job.client.job = None
            job.client = None
    return None


async def try_to_issue_job_to_client(client: Client, issue_job, job_given: trio.Event = None) -> None:
    for tube in client.watching:
        job_client = await try_to_issue_job(tube, issue_job)
        if isinstance(job_client, Client) and job_client == client:
            if job_given:
                job_given.set()
            break
