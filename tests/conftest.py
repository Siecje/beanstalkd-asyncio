import subprocess
import socket
import sys
import time

import pytest


@pytest.fixture(scope='session')
def server():
    cmd = [sys.executable, 'gofer.py']
    for _ in range(500):
        try:
            task = subprocess.Popen(cmd)
        except OSError as exc:
            if exc.errno == 48:
                time.sleep(1)
            else:
                raise
        else:
            break
    yield
    task.terminate()


def create_client_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', 10000)
    for _ in range(5):
        try:
            sock.connect(server_address)
        except ConnectionRefusedError:
            time.sleep(0.1)
        except OSError as exc:
            if exc.errno == 22:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                raise
        else:
            break
    return sock


@pytest.fixture(scope='function')
def client(server: None):
    sock = create_client_socket()
    yield sock
    sock.close()


@pytest.fixture(scope='function')
def client2(server: None):
    sock = create_client_socket()
    yield sock
    sock.close()
