import asyncio
from . import spec
from .exceptions import AMQPError


class Sender(object):
    def __init__(self, channel_id, protocol):
        self.channel_id = channel_id
        self.protocol = protocol

    def send_method(self, method):
        self.protocol.send_method(self.channel_id, method)


class FrameHandler(object):
    def __init__(self, synchroniser, sender):
        self.synchroniser = synchroniser
        self.sender = sender
        self.q = asyncio.Queue()
        self.is_waiting = False

    def ready(self):
        assert not self.is_waiting
        self.is_waiting = True
        asyncio.async(self.read_next())

    def enqueue(self, frame):
        self.q.put_nowait(frame)

    @asyncio.coroutine
    def read_next(self):
        frame = yield from self.q.get()
        self.is_waiting = False
        self.handle(frame)

    def handle(self, frame):
        try:
            self.synchroniser.check_expected(frame)
        except AMQPError:
            self.sender.send_Close(spec.UNEXPECTED_FRAME, "got an unexpected frame", *frame.payload.method_type)
            return

        try:
            handler = getattr(self, 'handle_' + type(frame).__name__)
        except AttributeError:
            handler = getattr(self, 'handle_' + type(frame.payload).__name__)

        handler(frame)
