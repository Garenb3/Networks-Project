
"""
Client for the Live Quiz Arena project.

Connects to the quiz server using TCP and UDP. TCP is used for
registration and reliable communication, while UDP is used to
receive lightweight updates such as question countdowns.

Separate threads listen for incoming TCP and UDP messages.
"""

import argparse
import socket
import threading
from typing import Any
import protocol as P


DEFAULT_SERVER_PORT = 5555
UDP_BUFFER_SIZE = 2048


class QuizClient:
    """Manage the client's sockets and communication with the server."""

    def __init__(self, server_host: str, server_port: int,
                 name: str) -> None:
        """
        Prepare a client with its server details and communication state.

        Args:
            server_host: IP address or hostname of the server.
            server_port: TCP port used by the server.
            name: Username chosen by the client.
        """
        self.server_host = server_host
        self.server_port = server_port
        self.name = name

        self.tcp_socket: socket.socket | None = None
        self.udp_socket: socket.socket | None = None
        self.reader: P.MessageReader | None = None
        self.udp_port: int | None = None

        self.client_id: str | None = None
        self.registered = False
        self.running = False

        # Signals when registration succeeds, fails, or the connection closes.
        self.registration_event = threading.Event()

        # Prevents multiple threads from writing to the TCP socket at once.
        self.send_lock = threading.Lock()

    def connect(self) -> None:
        """
        Open the UDP and TCP sockets and register with the server.

        The UDP socket is bound first so its port can be included
        in the TCP registration request.

        Raises:
            OSError: If a socket operation or connection fails.
            ConnectionError: If registration is rejected.
            TimeoutError: If the server does not respond in time.
        """
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind(("0.0.0.0", 0))
            self.udp_socket.settimeout(1)
            self.udp_port = self.udp_socket.getsockname()[1]

            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Limit the time spent establishing the initial connection.
            self.tcp_socket.settimeout(5)
            self.tcp_socket.connect((self.server_host, self.server_port))
            self.tcp_socket.settimeout(None)

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

        except (OSError, ConnectionError, TimeoutError):
            self.quit()
            raise

    def send_tcp(self, message: dict[str, Any]) -> None:
        """
        Send a message to the server over TCP.

        Args:
            message: The dictionary to encode and send.

        Raises:
            ConnectionError: If the TCP socket is unavailable.
            OSError: If sending fails.
        """
        if self.tcp_socket is None:
            raise ConnectionError("TCP connection is not available")

        with self.send_lock:
            self.tcp_socket.sendall(P.encode(message))

    def wait_for_registration(self, timeout: float = 5) -> None:
        """
        Wait for the server to confirm client registration.

        Args:
            timeout: Maximum time to wait in seconds.

        Raises:
            TimeoutError: If no registration response is received.
            ConnectionError: If registration fails.
        """
        if not self.registration_event.wait(timeout):
            raise TimeoutError("The server did not respond in time.")

        if not self.registered:
            raise ConnectionError(
                "The server did not register this client.")

    def _tcp_receive_loop(self) -> None:
        """
        Listen for messages sent by the server over TCP.

        Acknowledgements confirm registration, while error messages
        indicate that a request was rejected. Other messages are
        printed until the quiz interface is integrated.
        """
        try:
            while self.running:
                if self.reader is None:
                    break

                message = self.reader.read()

                if message is None:
                    break

                msg_type = message.get("type")

                if msg_type == P.Ack:
                    if message.get("for") == P.Register:
                        self.client_id = message.get("client_id")
                        self.registered = bool(self.client_id)

                        if self.registered:
                            print("\n[Server] Registration successful!")
                            print(f"[Server] Client ID: {self.client_id}")

                        self.registration_event.set()

                elif msg_type == P.Error:
                    print(
                        "\n[Server] Error:",
                        message.get("reason", "Unknown error"))
                    self.registration_event.set()
                    break

                else:
                    print("\n[Server] Received:", message)

        except (OSError, ValueError) as e:
            if self.running:
                print("\n[Connection] Error:", e)

        finally:
            self.running = False
            self.registration_event.set()

    def _udp_receive_loop(self) -> None:
        """
        Listen for incoming UDP datagrams from the server.

        Each datagram contains one JSON message. UDP does not use
        the TCP newline-based message framing.
        """
        while self.running:
            try:
                if self.udp_socket is None:
                    break

                data, address = self.udp_socket.recvfrom(UDP_BUFFER_SIZE)

                message = P.decode(data)
                print(f"\n[UDP] Received from {address}: {message}")

            except socket.timeout:
                continue

            except (OSError, ValueError) as e:
                if self.running:
                    print("\n[UDP] Error:", e)
                break

    def quit(self) -> None:
        """
        Notify the server that the client is leaving and close its sockets.

        The sockets are closed even if sending the quit message fails.
        """
        if self.running:
            try:
                self.send_tcp({"type": P.Quit})
            except OSError:
                pass

        self.running = False
        self.registration_event.set()

        if self.udp_socket is not None:
            try:
                self.udp_socket.close()
            except OSError:
                pass

        if self.tcp_socket is not None:
            try:
                self.tcp_socket.close()
            except OSError:
                pass


def run_terminal(client: QuizClient) -> None:
    """
    Connect the client and display the basic terminal menu.

    Args:
        client: The client instance to run.
    """
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


def main() -> None:
    """Read command-line arguments and start the client."""
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True, help="Server IP address")
    parser.add_argument("--port", type=int, default=DEFAULT_SERVER_PORT)
    parser.add_argument("--name", default="anonymous")
    args = parser.parse_args()

    client = QuizClient(args.host, args.port, args.name)
    run_terminal(client)


if __name__ == "__main__":
    main()
