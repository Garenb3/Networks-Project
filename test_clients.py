
"""
Concurrent client test for the Live Quiz Arena project.

Creates multiple simulated clients that connect to the server
at approximately the same time. Each client registers using TCP
and provides its UDP port. The test reports successful and failed
registrations and closes all client sockets afterward.
"""

import argparse
import socket
import threading
import protocol as P


DEFAULT_SERVER_PORT = 5555


class SimulatedClient:
    """Represent a client used for testing server registration."""

    def __init__(self, host: str, port: int, name: str) -> None:
        """
        Prepare a simulated client.

        Args:
            host: IP address or hostname of the server.
            port: TCP port used by the server.
            name: Username of the simulated client.
        """
        self.host = host
        self.port = port
        self.name = name

        self.tcp_socket: socket.socket | None = None
        self.udp_socket: socket.socket | None = None
        self.client_id: str | None = None

    def connect_and_register(self) -> str:
        """
        Connect to the server and request client registration.

        The UDP socket is bound first so its port can be included
        in the TCP registration message.

        Returns:
            The client ID assigned by the server.

        Raises:
            OSError: If a socket operation or connection fails.
            ConnectionError: If registration fails or the server
                              sends an unexpected response.
        """
        self.udp_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind(("0.0.0.0", 0))
        udp_port = self.udp_socket.getsockname()[1]

        self.tcp_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.settimeout(5)
        self.tcp_socket.connect((self.host, self.port))

        reader = P.MessageReader(self.tcp_socket)

        self.tcp_socket.sendall(P.encode({
            "type": P.Register,
            "name": self.name,
            "udp_port": udp_port
        }))

        response = reader.read()

        if response is None:
            raise ConnectionError("Server closed the connection.")

        if response.get("type") == P.Error:
            raise ConnectionError(
                response.get("reason", "Registration failed."))

        if (response.get("type") != P.Ack
                or response.get("for") != P.Register):
            raise ConnectionError("Unexpected server response.")

        client_id = response.get("client_id")

        if not isinstance(client_id, str) or not client_id:
            raise ConnectionError("No valid client ID received.")

        self.client_id = client_id
        return client_id

    def close(self) -> None:
        """
        Notify the server that the client is leaving and close its sockets.

        Socket cleanup is performed even if sending the quit message
        fails or registration was unsuccessful.
        """
        if self.tcp_socket is not None:
            try:
                self.tcp_socket.sendall(P.encode({"type": P.Quit}))
            except OSError:
                pass

            try:
                self.tcp_socket.close()
            except OSError:
                pass

        if self.udp_socket is not None:
            try:
                self.udp_socket.close()
            except OSError:
                pass


def run_test(host: str, port: int, client_count: int) -> None:
    """
    Run several simulated clients concurrently.

    A barrier makes all worker threads wait until they are ready
    before attempting to connect to the server.

    Args:
        host: IP address or hostname of the server.
        port: TCP port used by the server.
        client_count: Number of clients to simulate.
    """
    barrier = threading.Barrier(client_count)

    results: list[bool] = []
    results_lock = threading.Lock()

    def client_worker(number: int) -> None:
        """Connect, register, and close one simulated client."""
        client = SimulatedClient(host, port, f"TestClient{number}")

        try:
            barrier.wait(timeout=10)

            client_id = client.connect_and_register()

            print(f"[SUCCESS] Client {number} registered. ID: {client_id}")

            with results_lock:
                results.append(True)

            # Keep the connection open briefly to test concurrency.
            threading.Event().wait(3)

        except Exception as e:
            print(f"[FAILED] Client {number}: {e}")

            with results_lock:
                results.append(False)

        finally:
            client.close()

    threads = []

    for i in range(client_count):
        thread = threading.Thread(target=client_worker, args=(i + 1,))

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    successful = results.count(True)
    failed = results.count(False)

    print("\n========== TEST RESULTS ==========")
    print("Total clients:", client_count)
    print("Successful registrations:", successful)
    print("Failed registrations:", failed)
    print("==================================")


def main() -> None:
    """Read command-line arguments and start the concurrent test."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True, help="Server IP address")
    parser.add_argument("--port", type=int, default=DEFAULT_SERVER_PORT)
    parser.add_argument("--clients", type=int, default=15)
    args = parser.parse_args()

    if args.clients < 1:
        parser.error("--clients must be at least 1")

    run_test(args.host, args.port, args.clients)


if __name__ == "__main__":
    main()
