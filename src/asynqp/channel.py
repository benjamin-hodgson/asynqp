import asyncio
import re
from . import frames
from . import spec
from . import queue
from . import exchange
from . import message
from .util import Synchroniser
from .exceptions import AMQPError


VALID_QUEUE_NAME_RE = re.compile(r'^(?!amq\.)(\w|[-.:])*$', flags=re.A)
VALID_EXCHANGE_NAME_RE = re.compile(r'^(?!amq\.)(\w|[-.:])+$', flags=re.A)


class Channel(object):
    """
    A Channel is a 'virtual connection' over which messages are sent and received.
    Several independent channels can be multiplexed over the same connection,
    so peers can perform several tasks concurrently while using a single socket.

    Multi-threaded applications will typically use a 'channel-per-thread' approach,
    though it is also acceptable to open several connections for a single thread.

    Attributes:
        channel.id: the numerical ID of the channel

    Methods:
        channel.declare_queue(name='', **kwargs): Declare a queue on the broker. This method is a coroutine.
        channel.close(): Close the channel. This method is a coroutine.
    """
    def __init__(self, id, synchroniser, sender, loop):
        self.id = id
        self.synchroniser = synchroniser
        self.sender = sender
        self.loop = loop
        self.closing = False

    @asyncio.coroutine
    def declare_exchange(self, name, type, *, durable=True, auto_delete=False, internal=False):
        """
        Declare an exchange on the broker. If the exchange does not exist, it will be created.
        This method is a coroutine.

        Arguments:
            name: the name of the exchange.
            type: the type of the exchange (usually one of 'fanout', 'direct', 'topic', or 'headers')
            durable: If true, the exchange will be re-created when the server restarts.
                     default: True
            auto_delete: If true, the exchange will be deleted when the last queue is un-bound from it.
                         default: False
            internal: If true, the exchange cannot be published to directly; it can only be bound to other exchanges.
                      default: False

        Return value:
            the new Exchange object.
        """
        if name == '':
            return exchange.Exchange(self.sender, name, 'direct', True, False, False)

        if not VALID_EXCHANGE_NAME_RE.match(name):
            raise ValueError("Invalid exchange name.\n"
                             "Valid names consist of letters, digits, hyphen, underscore, period, or colon, "
                             "and do not begin with 'amq.'")

        with (yield from self.synchroniser.sync(spec.ExchangeDeclareOK)) as fut:
            self.sender.send_ExchangeDeclare(name, type, durable, auto_delete, internal)
            yield from fut
            return exchange.Exchange(self.sender, name, type, durable, auto_delete, internal)

    @asyncio.coroutine
    def declare_queue(self, name='', *, durable=True, exclusive=False, auto_delete=False):
        """
        Declare a queue on the broker. If the queue does not exist, it will be created.
        This method is a coroutine.

        Arguments:
            name: the name of the queue.
                  Supplying a name of '' will create a queue with a unique name of the server's choosing.
                  default: ''
            durable: If true, the queue will be re-created when the server restarts.
                     default: True
            exclusive: If true, the queue can only be accessed by the current connection,
                       and will be deleted when the connection is closed.
                       default: False
            auto_delete: If true, the queue will be deleted when the last consumer is cancelled.
                         If there were never any conusmers, the queue won't be deleted.
                         default: False

        Return value:
            The new Queue object.
        """
        if not VALID_QUEUE_NAME_RE.match(name):
            raise ValueError("Not a valid queue name.\n"
                             "Valid names consist of letters, digits, hyphen, underscore, period, or colon, "
                             "and do not begin with 'amq.'")

        with (yield from self.synchroniser.sync(spec.QueueDeclareOK)) as fut:
            self.sender.send_QueueDeclare(name, durable, exclusive, auto_delete)
            name = yield from fut
            return queue.Queue(self.synchroniser, self.loop, self.sender, name, durable, exclusive, auto_delete)

    @asyncio.coroutine
    def close(self):
        """
        Close the channel by handshaking with the server.
        This method is a coroutine.
        """
        with (yield from self.synchroniser.sync(spec.ChannelCloseOK)) as fut:
            self.closing = True
            self.sender.send_Close(0, 'Channel closed by application', 0, 0)
            yield from fut


class ChannelFrameHandler(object):
    def __init__(self, protocol, channel_id, loop, connection_info):
        self.synchroniser = Synchroniser(loop)
        self.sender = ChannelMethodSender(channel_id, protocol, connection_info)
        self.channel = Channel(channel_id, self.synchroniser, self.sender, loop)
        self.message_builder = None

    def handle(self, frame):
        if isinstance(frame, frames.ContentHeaderFrame):
            self.handle_ContentHeader(frame)
            return
        if isinstance(frame, frames.ContentBodyFrame):
            self.handle_ContentBody(frame)
            return

        method_cls = type(frame.payload)
        if self.channel.closing and method_cls not in (spec.ChannelClose, spec.ChannelCloseOK):
            return

        try:
            self.synchroniser.check_expected(frame)
        except AMQPError:
            self.sender.send_Close(spec.UNEXPECTED_FRAME, "got a bad message", *frame.payload.method_type)
            return

        handle_name = method_cls.__name__
        try:
            handler = getattr(self, 'handle_' + handle_name)
        except AttributeError as e:
            raise AMQPError('No handler defined for {} on channel {}'.format(handle_name, self.channel.id)) from e
        else:
            handler(frame)

    def handle_ChannelOpenOK(self, frame):
        self.synchroniser.succeed()

    def handle_QueueDeclareOK(self, frame):
        self.synchroniser.succeed(frame.payload.queue)

    def handle_ExchangeDeclareOK(self, frame):
        self.synchroniser.succeed()

    def handle_QueueBindOK(self, frame):
        self.synchroniser.succeed()

    def handle_QueueDeleteOK(self, frame):
        self.synchroniser.succeed()

    def handle_BasicGetEmpty(self, frame):
        self.synchroniser.succeed()

    def handle_BasicGetOK(self, frame):
        payload = frame.payload
        self.message_builder = message.MessageBuilder(payload.delivery_tag, payload.redelivered, payload.exchange, payload.routing_key)

    def handle_BasicDeliver(self, frame):
        pass

    def handle_ContentHeader(self, frame):
        self.message_builder.set_header(frame.payload)

    def handle_ContentBody(self, frame):
        self.message_builder.add_body_chunk(frame.payload)
        if self.message_builder.done():
            self.synchroniser.succeed(self.message_builder.build())
            self.message_builder = None

    def handle_ChannelClose(self, frame):
        self.channel.closing = True
        self.sender.send_CloseOK()

    def handle_ChannelCloseOK(self, frame):
        self.synchroniser.succeed()


class ChannelMethodSender(object):
    def __init__(self, channel_id, protocol, connection_info):
        self.channel_id = channel_id
        self.protocol = protocol
        self.connection_info = connection_info

    def send_ExchangeDeclare(self, name, type, durable, auto_delete, internal):
        method = spec.ExchangeDeclare(0, name, type, False, durable, auto_delete, internal, False, {})
        self.protocol.send_method(self.channel_id, method)

    def send_QueueDeclare(self, name, durable, exclusive, auto_delete):
        self.protocol.send_method(self.channel_id, spec.QueueDeclare(0, name, False, durable, exclusive, auto_delete, False, {}))

    def send_QueueBind(self, queue_name, exchange_name, routing_key):
        self.protocol.send_method(self.channel_id, spec.QueueBind(0, queue_name, exchange_name, routing_key, False, {}))

    def send_QueueDelete(self, queue_name, if_unused, if_empty):
        self.protocol.send_method(self.channel_id, spec.QueueDelete(0, queue_name, if_unused, if_empty, False))

    def send_BasicPublish(self, exchange_name, routing_key, mandatory, immediate):
        method = spec.BasicPublish(0, exchange_name, routing_key, mandatory, immediate)
        self.protocol.send_method(self.channel_id, method)

    def send_BasicGet(self, queue_name, no_ack):
        self.protocol.send_method(self.channel_id, spec.BasicGet(0, queue_name, no_ack))

    def send_content(self, msg):
        header_payload = msg.header_payload(spec.BasicPublish.method_type[0])
        header_frame = frames.ContentHeaderFrame(self.channel_id, header_payload)
        self.protocol.send_frame(header_frame)

        for payload in msg.frame_payloads(self.connection_info.frame_max - 8):
            frame = frames.ContentBodyFrame(self.channel_id, payload)
            self.protocol.send_frame(frame)

    def send_Close(self, status_code, msg, class_id, method_id):
        self.protocol.send_method(self.channel_id, spec.ChannelClose(status_code, msg, class_id, method_id))

    def send_CloseOK(self):
        self.protocol.send_method(self.channel_id, spec.ChannelCloseOK())
