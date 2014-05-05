import json
from collections import OrderedDict
from datetime import datetime
from io import BytesIO
from . import amqptypes
from . import serialisation


class Message(object):
    """
    A basic message.

    Some of the constructor parameters are ignored by the AMQP broker and are provided
    just for the convenience of user applications. They are marked "for applications"
    in the list below.

    Constructor parameters:
        body: bytestring, string or dictionary representing the body of the message.
              Strings will be encoded according to the content_encoding parameter;
              dicts will be converted to a string using JSON.
    Keyword-only parameters:
        headers: a dictionary of message headers
        content_type: MIME content type
        content_encoding: MIME encoding (default: utf-8)
        delivery_mode: 1 for non-persistent, 2 for persistent
        priority: message priority - integer between 0 and 9
        correlation_id: correlation id of the message (for applications)
        reply_to: reply-to address (for applications)
        expiration: expiration specification (for applications)
        message_id: unique id of the message (for applications)
        timestamp: datetime of when the message was sent (default: datetime.now())
        type: message type (for applications)
        user_id: ID of the user sending the message (for applications)
        app_id: ID of the application sending the message (for applications)

    Attributes: same as constructor parameters.
    """
    property_types = OrderedDict(
        [("content_type", amqptypes.ShortStr),
         ("content_encoding", amqptypes.ShortStr),
         ("headers", amqptypes.Table),
         ("delivery_mode", amqptypes.Octet),
         ("priority", amqptypes.Octet),
         ("correlation_id", amqptypes.ShortStr),
         ("reply_to", amqptypes.ShortStr),
         ("expiration", amqptypes.ShortStr),
         ("message_id", amqptypes.ShortStr),
         ("timestamp", amqptypes.Timestamp),
         ("type", amqptypes.ShortStr),
         ("user_id", amqptypes.ShortStr),
         ("app_id", amqptypes.ShortStr)]
    )

    def __init__(self, body, *,
                 headers=None, content_type=None,
                 content_encoding=None, delivery_mode=None,
                 priority=None, correlation_id=None,
                 reply_to=None, expiration=None,
                 message_id=None, timestamp=None,
                 type=None, user_id=None,
                 app_id=None):
        if content_encoding is None:
            content_encoding = 'utf-8'

        if isinstance(body, dict):
            body = json.dumps(body)
            if content_type is None:
                content_type = 'application/json'
        elif content_type is None:
            content_type = 'application/octet-stream'

        if isinstance(body, bytes):
            self.body = body
        else:
            self.body = body.encode(content_encoding)

        timestamp = timestamp if timestamp is not None else datetime.now()

        self.properties = OrderedDict()
        for name, amqptype in self.property_types.items():
            value = locals()[name]
            if value is not None:
                value = amqptype(value)
            self.properties[name] = value

    def __eq__(self, other):
        return (self.body == other.body
                and self.properties == other.properties)

    def __getattr__(self, name):
        try:
            return self.properties[name]
        except KeyError as e:
            raise AttributeError from e

    def header_payload(self, class_id):
        return ContentHeaderPayload(class_id, len(self.body), list(self.properties.values()))

    # the total frame size will be 8 bytes larger than frame_body_size
    def frame_payloads(self, frame_body_size):
        frames = []
        remaining = self.body
        while remaining:
            frame = remaining[:frame_body_size]
            remaining = remaining[frame_body_size:]
            frames.append(frame)
        return frames


class ContentHeaderPayload(object):
    def __init__(self, class_id, body_length, properties):
        self.class_id = class_id
        self.body_length = body_length
        self.properties = properties

    def __eq__(self, other):
        return (self.class_id == other.class_id
                and self.body_length == other.body_length
                and self.properties == other.properties)

    def write(self, stream):
        stream.write(serialisation.pack_short(self.class_id))
        stream.write(serialisation.pack_short(0))  # weight
        stream.write(serialisation.pack_long_long(self.body_length))

        bytesio = BytesIO()

        property_flags = 0
        bitshift = 15

        for val in self.properties:
            if val is not None:
                property_flags |= (1 << bitshift)
                val.write(bytesio)
            bitshift -= 1

        stream.write(serialisation.pack_short(property_flags))
        stream.write(bytesio.getvalue())

    @classmethod
    def read(cls, raw):
        bytesio = BytesIO(raw)
        class_id = serialisation.read_short(bytesio)
        weight = serialisation.read_short(bytesio)
        assert weight == 0
        body_length = serialisation.read_long_long(bytesio)
        property_flags_short = serialisation.read_short(bytesio)

        properties = []
        for amqptype, flag in zip(Message.property_types.values(), bin(property_flags_short)[2:]):
            if flag == '1':
                properties.append(amqptype.read(bytesio))
            else:
                properties.append(None)

        return cls(class_id, body_length, properties)


class MessageBuilder(object):
    def __init__(self, delivery_tag, redelivered, exchange_name, routing_key):
        self.body = b''

    def set_header(self, header):
        self.body_length = header.body_length
        self.properties = {}
        for name, prop in zip(Message.property_types, header.properties):
            self.properties[name] = prop

    def add_body_chunk(self, chunk):
        self.body += chunk

    def done(self):
        return len(self.body) == self.body_length

    def build(self):
        return Message(self.body, **self.properties)
