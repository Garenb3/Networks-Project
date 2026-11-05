
"""
UDP server for the Live Quiz Arena project.

Sends lightweight updates, such as question countdowns, to
registered clients. UDP is used for updates that can tolerate
occasional packet loss. Each message is sent as one datagram.
"""

import json
import socket
import threading
from typing import Any


serverPort = 7777
serverHost = "0.0.0.0"


class UDP_Server:
    """
    Manage the UDP socket used to send updates to clients.

    Unlike TCP, UDP does not establish a separate connection
    for each client. The same socket sends datagrams to all clients.
    """

    def __init__(self) -> None:
        """Create and bind the server's UDP socket."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((serverHost, serverPort))
        self.lock = threading.Lock()

    def send_udp(self, message: dict[str, Any],
                 address: tuple[str, int]) -> None:
        """
        Send one JSON message to a client's UDP address.

        Args:
            message: The message to encode and send.
            address: The client's IP address and UDP port.

        Returns:
            None.
        """
        data = json.dumps(
            message, separators=(",", ":")).encode("utf-8")

        with self.lock:
            self.socket.sendto(data, address)

    def broadcast_udp(self, clients: list[dict[str, Any]],
                      message: dict[str, Any]) -> None:
        """
        Send the same UDP message to multiple clients.

        Args:
            clients: A list of registered client records.
            message: The message to broadcast.

        Returns:
            None.
        """
        for client in clients:
            try:
                self.send_udp(message, client["udp_address"])
            except OSError:
                pass

    def close(self) -> None:
        """Close the UDP socket."""
        with self.lock:
            self.socket.close()
