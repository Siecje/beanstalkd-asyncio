import socket


def receive_data(socket: socket.socket, length: int, timeout_s: int = 3) -> bytes:
    socket.settimeout(timeout_s)
    amount_received = 0
    all_data = b''
    while amount_received < length:
        data = socket.recv(16)
        if data == b'':
            break
        amount_received += len(data)
        all_data += data

    return all_data
