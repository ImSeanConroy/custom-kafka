import socket

# -------------------------------
# Configuration
# -------------------------------
HOST = "127.0.0.1"
PORT = 9092

# -------------------------------
# Kafka API keys
# -------------------------------
API_KEYS = {
    0: "Produce",
    1: "Fetch",
    3: "Metadata",
    18: "ApiVersions"
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
            
            request = parse_request(client_socket)
            if request:
                api_key, api_version, correlation_id, _ = request
                print("API:", API_KEYS.get(api_key, "Unknown"))
                send_response(client_socket, correlation_id)

            client_socket.close()

if __name__ == "__main__":
    main()