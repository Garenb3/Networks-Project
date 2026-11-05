
"""
TCP server for the Live Quiz Arena project.

Accepts client connections, registers clients, receives and processes
TCP messages, and sends responses. Each connected client is handled
by a separate thread. The registered clients are also used by the
UDP server to send countdown updates.
"""

import socket
import threading
import time
from typing import Any
import protocol as P
from udp_server import UDP_Server


serverPort = 5555
serverHost = "0.0.0.0"

# Shared client information accessed by multiple client threads.
clientLock = threading.Lock()
clients: dict[str, dict[str, Any]] = {}

# Maximum time a client can remain inactive.
timeout = 60

# Shared UDP server instance.
udp_server: UDP_Server | None = None


def send_tcp(cid: str, message: dict[str, Any]) -> bool:
    """
    Send a message to a registered client over TCP.

    Args:
        cid: The ID of the client receiving the message.
        message: The message to encode and send.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    with clientLock:
        client = clients.get(cid)

    if client is None:
        return False

    # Prevent different threads from writing to the same socket at once.
    with client["lock"]:
        try:
            client["connection"].sendall(P.encode(message))
            return True
        except OSError:
            return False


def send_error(connection: socket.socket, cid: str | None,
               reason: str) -> None:
    """
    Send an error message to a client.

    If the client has registered, use send_tcp() so that the
    per-client sending lock is respected. Otherwise, send directly.

    Args:
        connection: The client's TCP socket.
        cid: The client's ID, or None if not registered.
        reason: A description of the error.
    """
    message = {"type": P.Error, "reason": reason}

    if cid is None:
        try:
            connection.sendall(P.encode(message))
        except OSError:
            pass
    else:
        send_tcp(cid, message)


def tcp_thread_runner(connection: socket.socket,
                      address: tuple[str, int]) -> None:
    """
    Handle all TCP communication with one connected client.

    Reads complete messages using MessageReader, processes them,
    and closes the connection when the client quits, an error occurs,
    or the client exceeds the inactivity timeout.

    Args:
        connection: The client's TCP socket.
        address: The client's IP address and port.

    Returns:
        None.
    """
    reader = P.MessageReader(connection)
    cid = None
    last_heard = time.time()
    connection.settimeout(15)

    try:
        while True:
            try:
                message = reader.read()
            except socket.timeout:
                if time.time() - last_heard > timeout:
                    send_error(
                        connection, cid, "Client exceeded timeout limit")
                    break
                continue
            except ValueError as e:
                send_error(connection, cid, str(e))
                break

            if message is None:
                break

            last_heard = time.time()

            try:
                cid, running = tcp_process(message, connection, address, cid)
            except ValueError as e:
                send_error(connection, cid, str(e))
                break

            if not running:
                break

    except OSError:
        pass
    finally:
        try:
            connection.close()
        except OSError:
            pass
        if cid is not None:
            with clientLock:
                clients.pop(cid, None)
            print(f"Client at {address} disconnected")


def tcp_process(message: dict[str, Any], connection: socket.socket,
                address: tuple[str, int],
                cid: str | None) -> tuple[str | None, bool]:
    """
    Process a decoded TCP message.

    Currently handles registration and quitting. Room and quiz
    operations will be connected to the corresponding functions
    when the quiz logic is integrated.

    Args:
        message: The decoded JSON message.
        connection: The client's TCP socket.
        address: The client's IP address and port.
        cid: The client's ID, or None if not registered.

    Returns:
        A tuple containing the client ID and whether the handler
        should continue running.

    Raises:
        ValueError: If the message is invalid or unexpected.
    """
    msg_type = message["type"]
    print(f"Got {msg_type}")

    if msg_type == P.Register:
        if cid is not None:
            raise ValueError("Client has already registered")

        udp_port = message.get("udp_port")
        name = message.get("name", "anonymous")

        if (type(udp_port) is not int
                or udp_port < 1 or udp_port > 65535):
            raise ValueError(
                "Registering requires an integer udp_port "
                "in [1, 65535]")

        if (not isinstance(name, str) or not name.strip()
                or len(name.strip()) > 35):
            raise ValueError(
                "Name must be a non-empty string of "
                "maximum 35 characters")

        cid = f"{address}"

        with clientLock:
            clients[cid] = {
                "tcp_address": address,
                "udp_address": (address[0], udp_port),
                "connection": connection,
                "lock": threading.Lock(),
                "name": name.strip()
            }

        if not send_tcp(cid, {
            "type": P.Ack,
            "for": P.Register,
            "client_id": cid
        }):
            return cid, False

        print(f"Client at {address} successfully registered")
        return cid, True

    if cid is None:
        raise ValueError(f"Received '{msg_type}' before client registration")

    if msg_type == P.Quit:
        print(f"Client at {address} quit")
        return cid, False

    # These operations will be connected during quiz integration.
    if msg_type in (P.Create, P.Join, P.Answer, P.Score):
        raise ValueError(f"Message type '{msg_type}' is not integrated yet")

    raise ValueError(f"Unknown message type: {msg_type}")


def tcp_loop() -> None:
    """
    Start the TCP and UDP servers and accept incoming TCP clients.

    A new thread is created for each accepted TCP connection.
    Both servers remain active until the program is stopped.

    Returns:
        None.
    """
    global udp_server

    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serverSocket.bind((serverHost, serverPort))
    serverSocket.listen()

    udp_server = UDP_Server()
    print("TCP and UDP servers are running.")

    try:
        while True:
            connectionSocket, address = serverSocket.accept()
            print(f"Accepted client at {address}")

            thread = threading.Thread(
                target=tcp_thread_runner,
                args=(connectionSocket, address),
                daemon=True)
            thread.start()

    except KeyboardInterrupt:
        print("Shutting down servers.")

    finally:
        serverSocket.close()

        # Close active client connections during server shutdown.
        with clientLock:
            active_clients = list(clients.values())
            clients.clear()

        for client in active_clients:
            try:
                client["connection"].close()
            except OSError:
                pass

        if udp_server is not None:
            udp_server.close()


if __name__ == "__main__":
    tcp_loop()
