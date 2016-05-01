import json
from collections import OrderedDict
from datetime import datetime
from io import BytesIO
from . import amqptypes
from . import serialisation


class Message(object):
    """
    An AMQP Basic message.

    Some of the constructor parameters are ignored by the AMQP broker and are provided
    just for the convenience of user applications. They are marked "for applications"
    in the list below.

    :param body: :func:`bytes` , :class:`str` or :class:`dict` representing the body of the message.
        Strings will be encoded according to the content_encoding parameter;
        dicts will be converted to a string using JSON.
    :param dict headers: a dictionary of message headers
    :param str content_type: MIME content type
        (defaults to 'application/json' if :code:`body` is a :class:`dict`,
        or 'application/octet-stream' otherwise)
    :param str content_encoding: MIME encoding (defaults to 'utf-8')
    :param int delivery_mode: 1 for non-persistent, 2 for persistent
    :param int priority: message priority - integer between 0 and 9
    :param str correlation_id: correlation id of the message *(for applications)*
    :param str reply_to: reply-to address *(for applications)*
    :param str expiration: expiration specification *(for applications)*
    :param str message_id: unique id of the message *(for applications)*
    :param datetime.datetime timestamp: :class:`~datetime.datetime` of when the message was sent
        (default: :meth:`datetime.now() <datetime.datetime.now>`)
    :param str type: message type *(for applications)*
    :param str user_id: ID of the user sending the message *(for applications)*
    :param str app_id: ID of the application sending the message *(for applications)*

    Attributes are the same as the constructor parameters.
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

        self._properties = OrderedDict()
        for name, amqptype in self.property_types.items():
            value = locals()[name]
            if value is not None:
                value = amqptype(value)
            self._properties[name] = value

    def __eq__(self, other):
        return (self.body == other.body
                and self._properties == other._properties)

    def __getattr__(self, name):
        try:
            return self._properties[name]
        except KeyError as e:
            raise AttributeError from e

    def __setattr__(self, name, value):
        amqptype = self.property_types.get(name)
        if amqptype is not None:
            self._properties[name] = value if isinstance(value, amqptype) else amqptype(value)
            return
        super().__setattr__(name, value)

    def json(self):
        """
        Parse the message body as JSON.

        :return: the parsed JSON.
        """
        return json.loads(self.body.decode(self.content_encoding))


class IncomingMessage(Message):
    """
    A message that has been delivered to the client.

    Subclass of :class:`Message`.

    .. attribute::delivery_tag

        The *delivery tag* assigned to this message by the AMQP broker.

    .. attribute::exchange_name

        The name of the exchange to which the message was originally published.

    .. attribute::routing_key

        The routing key under which the message was originally published.
    """
    def __init__(self, *args, sender, delivery_tag, exchange_name, routing_key, **kwargs):
        super().__init__(*args, **kwargs)
        self.sender = sender
        self.delivery_tag = delivery_tag
        self.exchange_name = exchange_name
        self.routing_key = routing_key

    def ack(self):
        """
        Acknowledge the message.
        """
        self.sender.send_BasicAck(self.delivery_tag)

    def reject(self, *, requeue=True):
        """
        Reject the message.

        :keyword bool requeue: if true, the broker will attempt to requeue the
            message and deliver it to an alternate consumer.
        """
        self.sender.send_BasicReject(self.delivery_tag, requeue)


def get_header_payload(message, class_id):
    return ContentHeaderPayload(class_id, len(message.body), list(message._properties.values()))


# NB: the total frame size will be 8 bytes larger than frame_body_size
def get_frame_payloads(message, frame_body_size):
    frames = []
    remaining = message.body
    while remaining:
        frame = remaining[:frame_body_size]
        remaining = remaining[frame_body_size:]
        frames.append(frame)
    return frames


class ContentHeaderPayload(object):
    synchronous = True

    def __init__(self, class_id, body_length, properties):
        self.class_id = class_id
        self.body_length = body_length
        self.properties = properties

    def __eq__(self, other):
        return (self.class_id == other.class_id
                and self.body_length == other.body_length
                and self.properties == other.properties)

    def write(self, stream):
        stream.write(serialisation.pack_unsigned_short(self.class_id))
        stream.write(serialisation.pack_unsigned_short(0))  # weight
        stream.write(serialisation.pack_unsigned_long_long(self.body_length))

        bytesio = BytesIO()

        property_flags = 0
        bitshift = 15

        for val in self.properties:
            if val is not None:
                property_flags |= (1 << bitshift)
                val.write(bytesio)
            bitshift -= 1

        stream.write(serialisation.pack_unsigned_short(property_flags))
        stream.write(bytesio.getvalue())

    @classmethod
    def read(cls, raw):
        bytesio = BytesIO(raw)
        class_id = serialisation.read_unsigned_short(bytesio)
        weight = serialisation.read_unsigned_short(bytesio)
        assert weight == 0
        body_length = serialisation.read_unsigned_long_long(bytesio)
        property_flags_short = serialisation.read_unsigned_short(bytesio)

        properties = []

        for i, amqptype in enumerate(Message.property_types.values()):
            pos = 15 - i  # We started from `content_type` witch has pos==15
            if property_flags_short & (1 << pos):
                properties.append(amqptype.read(bytesio))
            else:
                properties.append(None)

        return cls(class_id, body_length, properties)

    def __repr__(self):
        return "<ContentHeaderPayload {} {} {}>".format(
            self.class_id, self.body_length, self.properties)


class MessageBuilder(object):
    def __init__(self, sender, delivery_tag, redelivered, exchange_name, routing_key, consumer_tag=None):
        self.sender = sender
        self.delivery_tag = delivery_tag
        self.body = b''
        self.consumer_tag = consumer_tag
        self.exchange_name = exchange_name
        self.routing_key = routing_key

    def set_header(self, header):
        self.body_length = header.body_length
        self.properties = {}
        for name, prop in zip(IncomingMessage.property_types, header.properties):
            self.properties[name] = prop

    def add_body_chunk(self, chunk):
        self.body += chunk

    def done(self):
        return len(self.body) == self.body_length

    def build(self):
        return IncomingMessage(
            self.body,
            sender=self.sender,
            delivery_tag=self.delivery_tag,
            exchange_name=self.exchange_name,
            routing_key=self.routing_key,
            **self.properties)
