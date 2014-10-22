import asyncio
from . import spec
from . import frames
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

    def handle(self, frame):
        try:
            meth = getattr(self, 'handle_' + type(frame).__name__)
        except AttributeError:
            meth = getattr(self, 'handle_' + type(frame.payload).__name__)

        meth(frame)


def create_reader_and_writer(handler):
    q = asyncio.Queue()
    reader = QueueReader(handler, q)
    writer = QueueWriter(q)
    return reader, writer


class QueueReader(object):
    def __init__(self, handler, q):
        self.handler = handler
        self.q = q
        self.is_waiting = False

    def ready(self):
        assert not self.is_waiting, "ready() got called while waiting for a frame to be read"
        self.is_waiting = True
        asyncio.async(self.read_next())

    @asyncio.coroutine
    def read_next(self):
        assert self.is_waiting, "a frame got read without ready() having been called"
        frame = yield from self.q.get()
        self.is_waiting = False
        self.handler.handle(frame)


class QueueWriter(object):
    def __init__(self, q):
        self.q = q

    def enqueue(self, frame):
        self.q.put_nowait(frame)


class Dispatcher(object):
    def __init__(self):
        self.queue_writers = {}
        self.closing = asyncio.Future()

    def add_writer(self, channel_id, writer):
        self.queue_writers[channel_id] = writer

    def remove_writer(self, channel_id):
        del self.queue_writers[channel_id]

    def dispatch(self, frame):
        if isinstance(frame, frames.HeartbeatFrame):
            return
        if self.closing.done() and not isinstance(frame.payload, (spec.ConnectionClose, spec.ConnectionCloseOK)):
            return
        writer = self.queue_writers[frame.channel_id]
        writer.enqueue(frame)
