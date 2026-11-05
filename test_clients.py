
import argparse
import socket
import threading
import protocol as P


DEFAULT_SERVER_PORT = 5555


class SimulatedClient:

    # Prepare a simulated client with its own TCP and UDP sockets.
    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name

        self.tcp_socket = None
        self.udp_socket = None
        self.client_id = None

    # Connect to the server and check whether registration succeeds.
    def connect_and_register(self):
        self.udp_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        self.udp_socket.bind(("0.0.0.0", 0))
        udp_port = self.udp_socket.getsockname()[1]

        self.tcp_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

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
                response.get("reason", "Registration failed.")
            )

        if (
            response.get("type") != P.Ack
            or response.get("for") != "register"
        ):
            raise ConnectionError(
                "Unexpected server response."
            )

        self.client_id = response.get("client_id")

        if not self.client_id:
            raise ConnectionError("No client ID received.")

        return self.client_id

    # Tell the server that the client is leaving and close its sockets.
    def close(self):
        if self.tcp_socket:
            try:
                self.tcp_socket.sendall(
                    P.encode({"type": P.Quit})
                )
            except OSError:
                pass

            self.tcp_socket.close()

        if self.udp_socket:
            self.udp_socket.close()


# Run several clients at the same time and count successful registrations.
def run_test(host, port, client_count):
    barrier = threading.Barrier(client_count)

    results = []
    results_lock = threading.Lock()

    def client_worker(number):
        client = SimulatedClient(
            host,
            port,
            f"TestClient{number}"
        )

        try:
            barrier.wait()

            client_id = client.connect_and_register()

            print(
                f"[SUCCESS] Client {number} registered. ID: {client_id}"
            )

            with results_lock:
                results.append(True)

            threading.Event().wait(3)

        except Exception as e:
            print(f"[FAILED] Client {number}: {e}")

            with results_lock:
                results.append(False)

        finally:
            client.close()

    threads = []

    for i in range(client_count):
        thread = threading.Thread(
            target=client_worker,
            args=(i + 1,)
        )

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


# Read the test settings and start the multi-client test.
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--host",
        required=True
    )

    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_SERVER_PORT
    )

    parser.add_argument(
        "--clients",
        type=int,
        default=15
    )

    args = parser.parse_args()

    if args.clients < 1:
        parser.error("--clients must be at least 1")

    run_test(
        args.host,
        args.port,
        args.clients
    )


if __name__ == "__main__":
    main()
