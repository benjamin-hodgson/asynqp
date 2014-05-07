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
