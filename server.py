import socket
import threading
import protocol as P

serverPort = 5555
serverHost = "0.0.0.0"


clientLock = threading.Lock()
clients = {}


def tcp_thread_runner(connection, address):
    reader = P.MessageReader(connection)
    cid = None
    try:
        while True:
            try:
                message = reader.read()
            except ValueError as e:
                try:
                    connection.sendall(P.encode(
                        {"type": P.Error, "reason": str(e)}))
                except OSError:
                    break
                continue
            if message is None:
                break
            try:
                cid = tcp_process(message, connection, address, cid)
            except ValueError as e:
                try:
                    connection.sendall(P.encode(
                        {"type": P.Error, "reason": str(e)}))
                except OSError:
                    break
    except (ConnectionResetError, OSError):
        connection.close()
    finally:
        try:
            connection.close()
        except OSError:
            pass
        if cid:
            with clientLock:
                clients.pop(cid, None)


def tcp_process(message, connection, address, cid) -> str:
    msg_type = message["type"]
    if msg_type == P.Register:
        if cid is not None:
            raise ValueError("Client has already registered")
        udp_port = message.get("udp_port")
        if (udp_port is None or type(udp_port) is not int
                or udp_port < 1 or udp_port > 65535):
            connection.sendall(P.encode({"type": P.Error,
                                        "reason": "Registering requires an integer udp_port in [1, 65535]"}))
            return cid
        cid = f"{address}"
        with clientLock:
            clients[cid] = {
                "tcp_address": address,
                "udp_address": (address[0], message.get("udp_port")),
                "name": message.get("name", "anonymous")
                }
        connection.sendall(P.encode(
            {"type": P.Ack, "for": "register", "client_id":  cid}))
    if cid is None:
        raise ValueError(f"Caught a '{msg_type}' message before 'register'")
    elif msg_type == P.Quit:
        connection.close()
    return cid


def tcp_loop():
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serverSocket.bind((serverHost, serverPort))
    serverSocket.listen()
    try:
        while True:
            connectionSocket, addr = serverSocket.accept()
            thread = threading.Thread(target=tcp_thread_runner,
                                      args=(connectionSocket, addr))
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        serverSocket.close()


if __name__ == "__main__":
    tcp_loop()
