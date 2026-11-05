import socket
import json

serverPort = 7777
serverHost = "0.0.0.0"


class UDP_Server:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((serverHost, serverPort))

    def send_udp(self, message, address):
        data = json.dumps(message, separators=(",", ":")).encode()
        self.socket.sendto(data, address)

    def broadcast_udp(self, clients, message):
        for client in clients:
            try:
                self.send_udp(message, client["udp_address"])
            except OSError:
                pass

    def close(self):
        self.socket.close()
