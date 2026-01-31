import os
import socket
import threading
from typing import Tuple

# -------------------------------
# Configuration
# -------------------------------
HOST = "127.0.0.1"
PORT = 9092
DATA_DIR = "data"

os.makedirs(DATA_DIR, exist_ok=True)

# -------------------------------
# Kafka API keys
# -------------------------------
API_PRODUCE = 0
API_FETCH = 1
API_METADATA = 3
API_VERSIONS = 18

# -------------------------------
# Topic metadata
# -------------------------------
topics = {
    "test": [0, 1]
}

# -------------------------------
# Protocol helpers
# -------------------------------
def parse_request(client_socket: socket.socket) -> tuple[int, int, int, bytes] | None:
    raw_len = client_socket.recv(4)
    if not raw_len:
        return None

    length = int.from_bytes(raw_len, "big")
    raw = client_socket.recv(length)

    api_key = int.from_bytes(raw[0:2], "big")
    api_version = int.from_bytes(raw[2:4], "big")
    correlation_id = int.from_bytes(raw[4:8], "big")
    payload = raw[8:]

    return api_key, api_version, correlation_id, payload

def send_response(client_socket: socket.socket, correlation_id: int, payload: str = b"") -> None:
    length = 4 + len(payload)
    response = (
        length.to_bytes(4, "big") +
        correlation_id.to_bytes(4, "big") +
        payload
    )
    client_socket.sendall(response)

# -------------------------------
# API versions
# -------------------------------
def handle_api_versions() -> bytes:
    apis = [API_PRODUCE, API_FETCH, API_METADATA, API_VERSIONS]
    payload = len(apis).to_bytes(4, "big")
    for api in apis:
        payload += (
            api.to_bytes(2, "big") +
            (0).to_bytes(2, "big") +
            (1).to_bytes(2, "big")
        )
    return payload

# -------------------------------
# Metadata
# -------------------------------
def handle_metadata() -> bytes:
    payload = len(topics).to_bytes(4, "big")
    for topic, partitions in topics.items():
        tb = topic.encode()
        payload += len(tb).to_bytes(2, "big") + tb
        payload += len(partitions).to_bytes(4, "big")
        for p in partitions:
            payload += p.to_bytes(4, "big")
    return payload

# -------------------------------
# Fetch
# -------------------------------
def handle_fetch(payload: bytes) -> bytes:
    topic_len = int.from_bytes(payload[0:2], "big")
    topic = payload[2:2 + topic_len].decode()
    partition = int.from_bytes(payload[2 + topic_len:6 + topic_len], "big")

    filename = f"{DATA_DIR}/{topic}_{partition}.log"
    if not os.path.exists(filename):
        return b""

    response = b""
    with open(filename, "rb") as f:
        while True:
            size = f.read(4)
            if not size:
                break
            msg_len = int.from_bytes(size, "big")
            response += size + f.read(msg_len)

    return response

# -------------------------------
# Client handler
# -------------------------------
def handle_client(client_socket: socket.socket, addr: Tuple[str, int]) -> None:
    try:
        request = parse_request(client_socket)
        if request:
            api_key, _, correlation_id, payload = request

            if api_key == API_VERSIONS:
                response_payload = handle_api_versions()
            elif api_key == API_METADATA:
                response_payload = handle_metadata()
            elif api_key == API_FETCH:
                response_payload = handle_fetch(payload)
            else:
                response_payload = b""
                     
            send_response(client_socket, correlation_id, response_payload)
    
    except Exception as e:
        print(f"[{addr[0]}:{addr[1]}] Error handling client: {e}")
        send_response(client_socket, 500, "Internal Server Error")
    finally:
        client_socket.close()

# -------------------------------
# Server loop
# -------------------------------
def main() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()

        print(f"Broker listening on {HOST}:{PORT}")
        print("Press Ctrl+C to stop the server.\n")

        while True:
            client_socket, addr = server_socket.accept()
            thread = threading.Thread(
                target=handle_client,
                args=(client_socket, addr),
                daemon=True)
            thread.start()

if __name__ == "__main__":
    main()