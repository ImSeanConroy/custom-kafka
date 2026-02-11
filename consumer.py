import socket
import time

HOST = "127.0.0.1"
PORT = 9092

API_KEY_FETCH = 1
API_VERSION = 0

TOPIC = "test"
PARTITION = 0 

correlation_id = 1

print("Starting consumer...")

while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))

        topic_bytes = TOPIC.encode()
        payload = (
            API_KEY_FETCH.to_bytes(2, "big") +
            API_VERSION.to_bytes(2, "big") +
            correlation_id.to_bytes(4, "big") +
            len(topic_bytes).to_bytes(2, "big") +
            topic_bytes +
            PARTITION.to_bytes(4, "big")
        )

        s.sendall(len(payload).to_bytes(4, "big") + payload)

        res_len = int.from_bytes(s.recv(4), "big")
        res = s.recv(res_len)
        data = res[4:]

        offset = 0
        while offset < len(data):
            msg_len = int.from_bytes(data[offset:offset+4], "big")
            offset += 4
            message = data[offset:offset+msg_len]
            offset += msg_len
            print("Message:", message.decode())

    correlation_id += 1
    time.sleep(1)
