import os
import socket
import threading
from typing import Tuple, Optional


class KafkaBroker:
    # -------------------------------
    # Kafka API keys
    # -------------------------------
    API_PRODUCE = 0
    API_FETCH = 1
    API_METADATA = 3
    API_VERSIONS = 18

    def __init__(self, host: str = "127.0.0.1", port: int = 9092, data_dir: str = "data"):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.topics = {"test": [0, 1]}

        os.makedirs(self.data_dir, exist_ok=True)

    # -------------------------------
    # Protocol helpers
    # -------------------------------
    def parse_request(self, client_socket: socket.socket) -> Optional[tuple[int, int, int, bytes]]:
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

    def send_response(self, client_socket: socket.socket, correlation_id: int, payload: bytes = b"") -> None:
        length = 4 + len(payload)
        response = (
            length.to_bytes(4, "big") +
            correlation_id.to_bytes(4, "big") +
            payload
        )
        client_socket.sendall(response)

    # -------------------------------
    # API handlers
    # -------------------------------
    def handle_api_versions(self) -> bytes:
        apis = [
            self.API_PRODUCE,
            self.API_FETCH,
            self.API_METADATA,
            self.API_VERSIONS,
        ]

        payload = len(apis).to_bytes(4, "big")

        for api in apis:
            payload += (
                api.to_bytes(2, "big") +
                (0).to_bytes(2, "big") +
                (1).to_bytes(2, "big")
            )

        return payload

    def handle_metadata(self) -> bytes:
        payload = len(self.topics).to_bytes(4, "big")

        for topic, partitions in self.topics.items():
            tb = topic.encode()
            payload += len(tb).to_bytes(2, "big") + tb
            payload += len(partitions).to_bytes(4, "big")

            for p in partitions:
                payload += p.to_bytes(4, "big")

        return payload

    def handle_produce(self, payload: bytes) -> bytes:
        topic_len = int.from_bytes(payload[0:2], "big")
        topic = payload[2:2 + topic_len].decode()
        idx = 2 + topic_len

        partition = int.from_bytes(payload[idx:idx + 4], "big")
        idx += 4

        msg_len = int.from_bytes(payload[idx:idx + 4], "big")
        idx += 4

        message = payload[idx:idx + msg_len]

        if topic not in self.topics or partition not in self.topics[topic]:
            return b"\x01"  # error

        filename = f"{self.data_dir}/{topic}_{partition}.log"
        with open(filename, "ab") as f:
            f.write(len(message).to_bytes(4, "big") + message)

        return b"\x00"

    def handle_fetch(self, payload: bytes) -> bytes:
        topic_len = int.from_bytes(payload[0:2], "big")
        topic = payload[2:2 + topic_len].decode()
        partition = int.from_bytes(payload[2 + topic_len:6 + topic_len], "big")

        filename = f"{self.data_dir}/{topic}_{partition}.log"

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
    def handle_client(self, client_socket: socket.socket, addr: Tuple[str, int]) -> None:
        try:
            request = self.parse_request(client_socket)

            if request:
                api_key, _, correlation_id, payload = request

                if api_key == self.API_VERSIONS:
                    response_payload = self.handle_api_versions()
                elif api_key == self.API_METADATA:
                    response_payload = self.handle_metadata()
                elif api_key == self.API_PRODUCE:
                    response_payload = self.handle_produce(payload)
                elif api_key == self.API_FETCH:
                    response_payload = self.handle_fetch(payload)
                else:
                    response_payload = b""

                self.send_response(client_socket, correlation_id, response_payload)

        except Exception as e:
            print(f"[{addr[0]}:{addr[1]}] Error: {e}")
            self.send_response(client_socket, 500, b"Internal Server Error")
        finally:
            client_socket.close()

    # -------------------------------
    # Server loop
    # -------------------------------
    def start(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            print(f"Broker listening on {self.host}:{self.port}")
            print("Press Ctrl+C to stop.\n")

            while True:
                client_socket, addr = server_socket.accept()

                thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, addr),
                    daemon=True,
                )
                thread.start()


if __name__ == "__main__":
    my_broker = KafkaBroker()
    my_broker.start()
