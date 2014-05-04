import asyncio
import json
from collections import OrderedDict
from datetime import datetime
from . import frames
from . import amqptypes
from .connection import Connection, ConnectionInfo
from .exceptions import AMQPError
from .protocol import AMQP, Dispatcher


@asyncio.coroutine
def connect(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', *, loop=None):
    """
    Connect to an AMQP server on the given host and port.
    Log in to the given virtual host using the supplied credentials.
    This function is a coroutine.

    Arguments:
        host - the host server to connect to.
            default: 'localhost'
        port - the port which the AMQP server is listening on.
            default: 5672
        username - the username to authenticate with.
            default: 'guest'
        password - the password to authenticate with.
            default: 'guest'
        virtual_host - the AMQP virtual host to connect to.
            default: '/'
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    dispatcher = Dispatcher(loop)
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher, loop), host=host, port=port)
    connection_info = ConnectionInfo(username, password, virtual_host)
    connection = Connection(loop, protocol, dispatcher, connection_info)

    protocol.send_protocol_header()

    yield from connection.opened
    return connection


class Message(object):
    def __init__(self, body, *,
                 headers=None,
                 content_type=None,
                 content_encoding='utf-8',
                 delivery_mode=None,
                 priority=None,
                 correlation_id=None,
                 reply_to=None,
                 expiration=None,
                 message_id=None,
                 timestamp=None,
                 type=None,
                 user_id=None,
                 app_id=None):
        self.properties = OrderedDict()

        if isinstance(body, dict):
            body = json.dumps(body)
            if not content_type:
                content_type = 'application/json'
        elif not content_type:
            content_type = 'application/octet-stream'

        if isinstance(body, bytes):
            self.body = body
        else:
            self.body = body.encode(content_encoding)

        for amqptype, name, value in [(amqptypes.ShortStr, "content_type", content_type),
                                      (amqptypes.ShortStr, "content_encoding", content_encoding),
                                      (amqptypes.Table, "headers", headers),
                                      (amqptypes.Octet, "delivery_mode", delivery_mode),
                                      (amqptypes.Octet, "priority", priority),
                                      (amqptypes.ShortStr, "correlation_id", correlation_id),
                                      (amqptypes.ShortStr, "reply_to", reply_to),
                                      (amqptypes.ShortStr, "expiration", expiration),
                                      (amqptypes.ShortStr, "message_id", message_id),
                                      (amqptypes.Timestamp, "timestamp", timestamp if timestamp is not None else datetime.now()),
                                      (amqptypes.ShortStr, "type", type),
                                      (amqptypes.ShortStr, "user_id", user_id),
                                      (amqptypes.ShortStr, "app_id", app_id)]:
            if value is not None:
                value = amqptype(value)
            self.properties[name] = value

    def __getattr__(self, name):
        return self.properties[name]

    def header_payload(self, class_id):
        return frames.ContentHeaderPayload(class_id, len(self.body), list(self.properties.values()))

    # the total frame size will be 8 bytes larger than frame_body_size
    def frame_payloads(self, frame_body_size):
        frames = []
        remaining = self.body
        while remaining:
            frame = remaining[:frame_body_size]
            remaining = remaining[frame_body_size:]
            frames.append(frame)
        return frames
