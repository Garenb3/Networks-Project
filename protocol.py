import json


Register = "register"
Join = "join"
Error = "error"
Quit = "quit"
Score = "score"
Answer = "answer"
Ack = "ack"
Countdown = "countdown"

Delimiter = b"\n"


def encode(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode() + Delimiter


def decode(message: bytes) -> dict:
    try:
        decoded = json.loads(message.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Decoding error: {e}")
    if not isinstance(decoded, dict):
        raise ValueError("Message must be a JSON object")
    if "type" not in decoded.keys():
        raise ValueError("Type missing from request")
    if not isinstance(decoded.get("type"), str):
        raise ValueError("Type must be a string")
    return decoded


class MessageReader:
    def __init__(self, sock, buffer_size=1024):
        self.sock = sock
        self.buffer_size = buffer_size
        self.buffer = b""

    def read(self):
        while Delimiter not in self.buffer:
            chunk = self.sock.recv(self.buffer_size)
            if not chunk:
                return None
            self.buffer += chunk
        line, self.buffer = self.buffer.split(Delimiter, 1)
        return decode(line)
