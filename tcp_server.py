import socket
import threading
import protocol as P
from udp_server import UDP_Server
import time

serverPort = 5555
serverHost = "0.0.0.0"


clientLock = threading.Lock()
clients = {}

timeout = 60


def send_tcp(cid, message):
    with clientLock:
        client = clients.get(cid)
    if client is None:
        return
    with client["lock"]:
        try:
            client["connection"].sendall(P.encode(message))
        except OSError:
            pass


def send_error(connection, cid, reason):
    message = {"type": P.Error, "reason": reason}
    if cid is None:
        try:
            connection.sendall(P.encode(message))
        except OSError:
            pass
    else:
        send_tcp(cid, message)


def tcp_thread_runner(connection, address):
    reader = P.MessageReader(connection)
    cid = None
    last_heard = time.time()
    connection.settimeout(5)
    try:
        while True:
            try:
                message = reader.read()
            except socket.timeout:
                if time.time() - last_heard > timeout:
                    send_error(connection, cid, "Client exceeded timeout limit")
            except ValueError as e:
                send_error(connection, cid, str(e))
                continue
            if message is None:
                break
            last_heard = time.time()
            try:
                cid, running = tcp_process(message, connection, address, cid)
                if not running:
                    break
            except ValueError as e:
                send_error(connection, cid, str(e))
                break
    except OSError:
        pass
    finally:
        try:
            connection.close()
        except OSError:
            pass
        if cid:
            with clientLock:
                clients.pop(cid, None)
            print(f"Client at {address} disconnected")


def tcp_process(message, connection, address, cid):
    msg_type = message["type"]
    if msg_type == P.Register:
        if cid is not None:
            raise ValueError("Client has already registered")
        udp_port = message.get("udp_port")
        name = message.get("name", "anonymous")
        if (udp_port is None or type(udp_port) is not int
                or udp_port < 1 or udp_port > 65535):
            raise ValueError(
                "Registering requires an integer udp_port in [1, 65535]")
        if not isinstance(name, str) or not name.strip() or len(name) > 35:
            raise ValueError("Name must be a string of maximum 35 characters")

        cid = f"{address}"
        with clientLock:
            clients[cid] = {
                "tcp_address": address,
                "udp_address": (address[0], udp_port),
                "connection": connection,
                "lock": threading.Lock(),
                "name": name
                }
        send_tcp(cid, {"type": P.Ack, "for": "register", "client_id":  cid})
        print(f"Client at {address} successfully registered")
        return cid, True
    if cid is None:
        raise ValueError(f"Caught a '{msg_type}' message before 'register'")
    elif msg_type == P.Quit:
        print(f"Client at {address} quit")
        return cid, False
    raise ValueError(f"Unknown message type: {msg_type}")


def tcp_loop():
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serverSocket.bind((serverHost, serverPort))
    serverSocket.listen()

    udp_server = UDP_Server()
    print("Tcp and UDP servers are running.")
    try:
        while True:
            connectionSocket, address = serverSocket.accept()
            print(f"Accepted client at {address}")
            thread = threading.Thread(target=tcp_thread_runner,
                                      args=(connectionSocket, address),
                                      daemon=True)
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        serverSocket.close()
        udp_server.close()


if __name__ == "__main__":
    tcp_loop()
