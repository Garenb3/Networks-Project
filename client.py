
import argparse
import socket
import threading
import protocol as P


DEFAULT_SERVER_PORT = 5555
UDP_BUFFER_SIZE = 2048


class QuizClient:

    # Set up the client and prepare its sockets and connection details.
    def __init__(self, server_host, server_port, name):
        self.server_host = server_host
        self.server_port = server_port
        self.name = name

        self.tcp_socket = None
        self.udp_socket = None
        self.reader = None

        self.client_id = None
        self.registered = False
        self.running = False

        self.registration_event = threading.Event()
        self.send_lock = threading.Lock()

    # Open the UDP and TCP sockets, then register with the server.
    def connect(self):
        self.udp_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        self.udp_socket.bind(("0.0.0.0", 0))
        self.udp_socket.settimeout(1)

        self.udp_port = self.udp_socket.getsockname()[1]

        self.tcp_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.tcp_socket.connect(
            (self.server_host, self.server_port)
        )

        self.reader = P.MessageReader(self.tcp_socket)
        self.running = True

        threading.Thread(
            target=self._tcp_receive_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=self._udp_receive_loop,
            daemon=True
        ).start()

        self.send_tcp({
            "type": P.Register,
            "name": self.name,
            "udp_port": self.udp_port
        })

        self.wait_for_registration()

    # Send a message to the server using TCP.
    def send_tcp(self, message):
        with self.send_lock:
            self.tcp_socket.sendall(P.encode(message))

    # Wait for the server to confirm that registration succeeded.
    def wait_for_registration(self, timeout=5):
        if not self.registration_event.wait(timeout):
            raise TimeoutError(
                "The server did not respond in time."
            )

        if not self.registered:
            raise ConnectionError(
                "The server did not register this client."
            )

    # Keep listening for messages coming from the server.
    def _tcp_receive_loop(self):
        try:
            while self.running:
                message = self.reader.read()

                if message is None:
                    break

                msg_type = message.get("type")

                if msg_type == P.Ack:
                    if message.get("for") == "register":
                        self.client_id = message.get("client_id")
                        self.registered = bool(self.client_id)

                        if self.registered:
                            print(
                                "\n[Server] Registration successful!"
                            )
                            print(
                                f"[Server] Client ID: {self.client_id}"
                            )

                        self.registration_event.set()

                elif msg_type == P.Error:
                    print(
                        "\n[Server] Error:",
                        message.get("reason", "Unknown error")
                    )

                    self.registration_event.set()

                else:
                    print("\n[Server] Received:", message)

        except (OSError, ValueError) as e:
            if self.running:
                print("\n[Connection] Error:", e)

        finally:
            self.running = False
            self.registration_event.set()

    # Listen for incoming UDP messages from the server.
    def _udp_receive_loop(self):
        while self.running:
            try:
                data, address = self.udp_socket.recvfrom(
                    UDP_BUFFER_SIZE
                )

                message = P.decode(data.rstrip(b"\n"))

                print("\n[UDP] Received:", message)

            except socket.timeout:
                continue

            except (OSError, ValueError) as e:
                if self.running:
                    print("\n[UDP] Error:", e)
                break

    # Send a quit message and close the client's sockets.
    def quit(self):
        if self.running:
            try:
                self.send_tcp({
                    "type": P.Quit
                })
            except OSError:
                pass

        self.running = False

        if self.udp_socket:
            self.udp_socket.close()

        if self.tcp_socket:
            self.tcp_socket.close()


# Start the client and keep it running until the user exits.
def run_terminal(client):
    try:
        client.connect()

        print("\nConnected to the quiz server.")
        print("Your client is registered.")
        print("UDP port:", client.udp_port)

        while client.running:
            print("\n1. Show connection information")
            print("2. Quit")

            choice = input("Choose an option: ")

            if choice == "1":
                print("\nServer:", client.server_host)
                print("TCP port:", client.server_port)
                print("UDP port:", client.udp_port)
                print("Client ID:", client.client_id)

            elif choice == "2":
                break

            else:
                print("Invalid choice.")

    except (OSError, ConnectionError, TimeoutError) as e:
        print("\nConnection failed:", e)

    finally:
        client.quit()


# Read the server details and start the client.
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--host",
        required=True,
        help="Server IP address"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_SERVER_PORT
    )

    parser.add_argument(
        "--name",
        default="anonymous"
    )

    args = parser.parse_args()

    client = QuizClient(
        args.host,
        args.port,
        args.name
    )

    run_terminal(client)


if __name__ == "__main__":
    main()
