import socket

HOST = '127.0.0.1'
PORT = 9092

def main() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()

        print(f"Server listening on {HOST}:{PORT}")
        print("Press Ctrl+C to stop the server.\n")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connected by {addr}")
            client_socket.close()
    

if __name__ == "__main__":
    main()