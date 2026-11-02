import json


Register = "register",
Join = "join",
Error = "error",
Quit = "quit",
Score = "score",
Answer = "answer",
Ack = "ack",
Countdown = "countdown"

delimeter = "b\n"


def encode(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode() + delimeter


def decode(message: bytes) -> dict:
    try:
        decoded = json.load(message)
    except json.JSONDecodeError as e:
        print(f"JSON conversion error: {e}")
    if "type" not in decoded.keys():
        raise ValueError("Type missing from request")


class MessageReader:
    def __init__(self, sock, buffer_size=1024):
        self.sock = sock
        self.buffer_size = buffer_size
        self.buffer = b""

    def read(self):
        while delimeter not in self.buffer:
            chunk = self.socket.recv(self.buffer_size)
            if not chunk:
                break
            self.buffer += chunk
        line, self.buffer = self.buffer.split(delimeter, 1)
        return line.decode()
