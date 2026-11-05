
"""
Application-layer protocol for the Live Quiz Arena project.

Defines message types and handles JSON encoding, decoding, and
TCP message framing. TCP is a byte stream, so messages are
separated using a newline delimiter.
"""

import json
import socket
from typing import Any


# Message types used by the client-server protocol.
Register = "register"
Create = "create"
Join = "join"
Error = "error"
Quit = "quit"
Score = "score"
Answer = "answer"
Ack = "ack"
Countdown = "countdown"

# TCP messages end with this delimiter.
Delimiter = b"\n"

# Maximum allowed size of one TCP message.
MaxMessageSize = 65536


def encode(message: dict[str, Any]) -> bytes:
    """
    Convert a message dictionary into newline-delimited JSON bytes.

    Args:
        message: The dictionary to encode.

    Returns:
        The encoded message, including its delimiter.
    """
    return json.dumps(
        message, separators=(",", ":")).encode("utf-8") + Delimiter


def decode(message: bytes) -> dict[str, Any]:
    """
    Decode a JSON message and validate its basic structure.

    Args:
        message: The received JSON bytes, without the delimiter.

    Returns:
        The decoded message dictionary.

    Raises:
        ValueError: If the message is not valid JSON, is not a
                    dictionary, or does not contain a valid type.
    """
    try:
        decoded = json.loads(message.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Decoding error: {e}") from e

    if not isinstance(decoded, dict):
        raise ValueError("Message must be a JSON object")

    if "type" not in decoded:
        raise ValueError("Type missing from request")

    if not isinstance(decoded["type"], str):
        raise ValueError("Type must be a string")

    return decoded


class MessageReader:
    """
    Read complete newline-delimited messages from a TCP socket.

    TCP does not preserve message boundaries. This class keeps
    received bytes in a buffer until a complete message is available.
    It also preserves any additional messages received in the same
    socket read for the next call to read().
    """

    def __init__(self, sock: socket.socket, buffer_size: int = 1024) -> None:
        """
        Initialize the TCP message reader.

        Args:
            sock: The connected TCP socket.
            buffer_size: Maximum number of bytes read at once.
        """
        self.sock = sock
        self.buffer_size = buffer_size
        self.buffer = b""

    def read(self) -> dict[str, Any] | None:
        """
        Read and decode the next complete TCP message.

        Returns:
            The decoded message, or None if the connection closes.

        Raises:
            ValueError: If a message exceeds the maximum size or
                        cannot be decoded.
            socket.timeout: If the socket's receive timeout expires.
        """
        while Delimiter not in self.buffer:
            chunk = self.sock.recv(self.buffer_size)

            if not chunk:
                return None

            self.buffer += chunk

            if len(self.buffer) > MaxMessageSize:
                raise ValueError("TCP message exceeds maximum size")

        line, self.buffer = self.buffer.split(Delimiter, 1)

        if len(line) > MaxMessageSize:
            raise ValueError("TCP message exceeds maximum size")

        return decode(line)
