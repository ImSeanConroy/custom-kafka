import socket
import time
import random
import string

HOST = "127.0.0.1"
PORT = 9092

API_KEY_PRODUCE = 0
API_VERSION = 0

TOPIC = "test"
PARTITION = 0


correlation_id = 1

print("Starting producer...")

while True:
    message = ''.join(random.choice(string.ascii_lowercase) for _ in range(10)).encode()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))

        topic_bytes = TOPIC.encode()
        payload = (
            API_KEY_PRODUCE.to_bytes(2, "big") +
            API_VERSION.to_bytes(2, "big") +
            correlation_id.to_bytes(4, "big") +
            len(topic_bytes).to_bytes(2, "big") +
            topic_bytes +
            PARTITION.to_bytes(4, "big") +
            len(message).to_bytes(4, "big") +
            message
        )

        s.sendall(len(payload).to_bytes(4, "big") + payload)

        res_len = int.from_bytes(s.recv(4), "big")
        res = s.recv(res_len)
        res_correlation_id = int.from_bytes(res[:4], "big")

        print(
            f"Produced message: {message.decode()} "
            f"(correlation_id={res_correlation_id})"
        )

    correlation_id += 1
    time.sleep(1)
