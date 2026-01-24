import socket

HOST = '127.0.0.1'
PORT = 9092

def parse_request(conn: socket.socket) -> tuple[int, bytes] | None:
    raw_len = conn.recv(4)
    if not raw_len:
        return None

    length = int.from_bytes(raw_len, "big")
    raw = conn.recv(length)

    correlation_id = int.from_bytes(raw[:4], "big")
    payload = raw[4:]
    return correlation_id, payload

def send_response(conn: socket.socket, correlation_id: int, payload: str = b"") -> None:
    length = 4 + len(payload)
    response = (
        length.to_bytes(4, "big") +
        correlation_id.to_bytes(4, "big") +
        payload
    )
    conn.sendall(response)

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
                correlation_id, _ = request
                send_response(client_socket, correlation_id)

            client_socket.close()
    
if __name__ == "__main__":
    main()