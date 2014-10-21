import asyncio
import re
from . import bases
from . import frames
from . import spec
from . import queue
from . import exchange
from . import message
from .util import Synchroniser
from .exceptions import UnhandledBasicReturn


VALID_QUEUE_NAME_RE = re.compile(r'^(?!amq\.)(\w|[-.:])*$', flags=re.A)
VALID_EXCHANGE_NAME_RE = re.compile(r'^(?!amq\.)(\w|[-.:])+$', flags=re.A)


class Channel(object):
    """
    Manage AMQP Channels.

    A Channel is a 'virtual connection' over which messages are sent and received.
    Several independent channels can be multiplexed over the same :class:`Connection`,
    so peers can perform several tasks concurrently while using a single socket.

    Channels are created using :meth:`Connection.open_channel() <Connection.open_channel>`.

    .. attribute::id

        the numerical ID of the channel
    """
    def __init__(self, consumers, id, synchroniser, sender, message_receiver, loop):
        self.consumers = consumers
        self.id = id
        self.synchroniser = synchroniser
        self.sender = sender
        self.message_receiver = message_receiver
        self.loop = loop
        self.closing = False
        self._basic_return_handler = None

    @asyncio.coroutine
    def declare_exchange(self, name, type, *, durable=True, auto_delete=False, internal=False):
        """
        Declare an :class:`Exchange` on the broker. If the exchange does not exist, it will be created.

        This method is a :ref:`coroutine <coroutine>`.

        :param str name: the name of the exchange.
        :param str type: the type of the exchange (usually one of ``'fanout'``, ``'direct'``, ``'topic'``, or ``'headers'``)
        :keyword bool durable: If true, the exchange will be re-created when the server restarts.
        :keyword bool auto_delete: If true, the exchange will be deleted when the last queue is un-bound from it.
        :keyword bool internal: If true, the exchange cannot be published to directly; it can only be bound to other exchanges.

        :return: the new :class:`Exchange` object.
        """
        if name == '':
            return exchange.Exchange(self.handler, self.synchroniser, self.sender, name, 'direct', True, False, False)

        if not VALID_EXCHANGE_NAME_RE.match(name):
            raise ValueError("Invalid exchange name.\n"
                             "Valid names consist of letters, digits, hyphen, underscore, period, or colon, "
                             "and do not begin with 'amq.'")

        self.sender.send_ExchangeDeclare(name, type, durable, auto_delete, internal)
        yield from self.synchroniser.await(spec.ExchangeDeclareOK)
        ex = exchange.Exchange(self.handler, self.synchroniser, self.sender, name, type, durable, auto_delete, internal)
        self.handler.ready()
        return ex

    @asyncio.coroutine
    def declare_queue(self, name='', *, durable=True, exclusive=False, auto_delete=False):
        """
        Declare a queue on the broker. If the queue does not exist, it will be created.

        This method is a :ref:`coroutine <coroutine>`.

        :param str name: the name of the queue.
            Supplying a name of '' will create a queue with a unique name of the server's choosing.

        :keyword bool durable: If true, the queue will be re-created when the server restarts.

        :keyword bool exclusive: If true, the queue can only be accessed by the current connection,
            and will be deleted when the connection is closed.

        :keyword bool auto_delete: If true, the queue will be deleted when the last consumer is cancelled.
            If there were never any conusmers, the queue won't be deleted.

        :return: The new :class:`Queue` object.
        """
        if not VALID_QUEUE_NAME_RE.match(name):
            raise ValueError("Not a valid queue name.\n"
                             "Valid names consist of letters, digits, hyphen, underscore, period, or colon, "
                             "and do not begin with 'amq.'")

        self.sender.send_QueueDeclare(name, durable, exclusive, auto_delete)
        name = yield from self.synchroniser.await(spec.QueueDeclareOK)
        q = queue.Queue(self.handler, self.consumers, self.synchroniser, self.loop, self.sender, self.message_receiver, name, durable, exclusive, auto_delete)
        self.handler.ready()
        return q

    @asyncio.coroutine
    def close(self):
        """
        Close the channel by handshaking with the server.

        This method is a :ref:`coroutine <coroutine>`.
        """
        self.closing = True
        self.sender.send_Close(0, 'Channel closed by application', 0, 0)
        yield from self.synchroniser.await(spec.ChannelCloseOK)

    @asyncio.coroutine
    def set_qos(self, prefetch_size=0, prefetch_count=0, apply_globally=False):
        """
        Specify quality of service by requesting that messages be pre-fetched
        from the server. Pre-fetching means that the server will deliver messages
        to the client while the client is still processing unacknowledged messages.

        This method is a :ref:`coroutine <coroutine>`.

        :param int prefetch_size: Specifies a prefetch window in bytes.
            Messages smaller than this will be sent from the server in advance.
            This value may be set to 0, which means "no specific limit".

        :param int prefetch_count: Specifies a prefetch window in terms of whole messages.

        :param bool apply_globally: If true, apply these QoS settings on a global level.
            The meaning of this is implementation-dependent. From the
            `RabbitMQ documentation <https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.qos.global>`_:

                RabbitMQ has reinterpreted this field. The original specification said:
                "By default the QoS settings apply to the current channel only.
                If this field is set, they are applied to the entire connection."
                Instead, RabbitMQ takes global=false to mean that the QoS settings should apply
                per-consumer (for new consumers on the channel; existing ones being unaffected) and
                global=true to mean that the QoS settings should apply per-channel.
        """
        self.sender.send_BasicQos(prefetch_size, prefetch_count, apply_globally)
        yield from self.synchroniser.await(spec.BasicQosOK)
        self.handler.ready()

    def set_return_handler(self, handler):
        self._basic_return_handler = handler


class ChannelFactory(object):
    def __init__(self, loop, protocol, dispatcher, connection_info):
        self.loop = loop
        self.protocol = protocol
        self.dispatcher = dispatcher
        self.connection_info = connection_info
        self.next_channel_id = 1

    @asyncio.coroutine
    def open(self):
        synchroniser = Synchroniser(self.loop)

        sender = ChannelMethodSender(self.next_channel_id, self.protocol, self.connection_info)
        consumers = queue.Consumers(self.loop)
        message_receiver = MessageReceiver(synchroniser, sender, consumers)
        channel = Channel(consumers, self.next_channel_id, synchroniser, sender, message_receiver, self.loop)
        handler = ChannelFrameHandler(synchroniser, sender, channel, consumers, message_receiver)
        channel.handler = handler
        consumers.handler = handler
        message_receiver.handler = handler

        self.dispatcher.add_handler(self.next_channel_id, handler)
        try:
            sender.send_ChannelOpen()
            handler.ready()
            yield from synchroniser.await(spec.ChannelOpenOK)
        except:
            self.dispatcher.remove_handler(self.next_channel_id)
            raise

        self.next_channel_id += 1
        handler.ready()
        return channel


class ChannelFrameHandler(bases.FrameHandler):
    def __init__(self, synchroniser, sender, channel, consumers, message_receiver):
        super().__init__(synchroniser, sender)
        self.channel = channel
        self.consumers = consumers
        self.message_receiver = message_receiver

    def handle(self, frame):
        if self.channel.closing and type(frame.payload) is not spec.ChannelCloseOK:
            return
        super().handle(frame)

    def handle_ChannelOpenOK(self, frame):
        self.synchroniser.notify(spec.ChannelOpenOK)

    def handle_QueueDeclareOK(self, frame):
        self.synchroniser.notify(spec.QueueDeclareOK, frame.payload.queue)

    def handle_ExchangeDeclareOK(self, frame):
        self.synchroniser.notify(spec.ExchangeDeclareOK)

    def handle_ExchangeDeleteOK(self, frame):
        self.synchroniser.notify(spec.ExchangeDeleteOK)

    def handle_QueueBindOK(self, frame):
        self.synchroniser.notify(spec.QueueBindOK)

    def handle_QueueUnbindOK(self, frame):
        self.synchroniser.notify(spec.QueueUnbindOK)

    def handle_QueuePurgeOK(self, frame):
        self.synchroniser.notify(spec.QueuePurgeOK)

    def handle_QueueDeleteOK(self, frame):
        self.synchroniser.notify(spec.QueueDeleteOK)

    def handle_BasicGetEmpty(self, frame):
        self.synchroniser.notify(spec.BasicGetEmpty, False)

    def handle_BasicGetOK(self, frame):
        asyncio.async(self.message_receiver.receive_getOK(frame))

    def handle_BasicConsumeOK(self, frame):
        self.synchroniser.notify(spec.BasicConsumeOK, frame.payload.consumer_tag)

    def handle_BasicCancelOK(self, frame):
        consumer_tag = frame.payload.consumer_tag
        self.synchroniser.notify(spec.BasicCancelOK)
        self.consumers.cancel(consumer_tag)

    def handle_BasicDeliver(self, frame):
        asyncio.async(self.message_receiver.receive_deliver(frame))

    def handle_ContentHeaderFrame(self, frame):
        asyncio.async(self.message_receiver.receive_header(frame))

    def handle_ContentBodyFrame(self, frame):
        asyncio.async(self.message_receiver.receive_body(frame))

    def handle_ChannelClose(self, frame):
        self.channel.closing = True
        self.sender.send_CloseOK()

    def handle_ChannelCloseOK(self, frame):
        self.synchroniser.notify(spec.ChannelCloseOK)

    def handle_BasicQosOK(self, frame):
        self.synchroniser.notify(spec.BasicQosOK)

    def handle_BasicReturn(self, frame):
        # schedule reading of the next frame, then raise the exception
        self.ready()

        if self.channel._basic_return_handler is not None:
            self.channel._basic_return_handler({
                "reply_code": frame.payload.reply_code,
                "message": frame.payload.reply_text,
                "exchange_name": frame.payload.exchange,
                "routing_key": frame.payload.routing_key
            })
        else:
            raise UnhandledBasicReturn(frame.payload.reply_code, frame.payload.reply_text, frame.payload.exchange, frame.payload.routing_key)


class MessageReceiver(object):
    def __init__(self, synchroniser, sender, consumers):
        self.synchroniser = synchroniser
        self.sender = sender
        self.consumers = consumers
        self.message_builder = None

    @asyncio.coroutine
    def receive_getOK(self, frame):
        self.synchroniser.notify(spec.BasicGetOK, True)
        payload = frame.payload
        self.message_builder = message.MessageBuilder(
            self.sender,
            payload.delivery_tag,
            payload.redelivered,
            payload.exchange,
            payload.routing_key
        )
        self.handler.ready()

    @asyncio.coroutine
    def receive_deliver(self, frame):
        payload = frame.payload
        self.message_builder = message.MessageBuilder(
            self.sender,
            payload.delivery_tag,
            payload.redelivered,
            payload.exchange,
            payload.routing_key,
            payload.consumer_tag
        )
        self.handler.ready()
        yield from self.synchroniser.await(frames.ContentHeaderFrame)
        tag, msg = yield from self.synchroniser.await(frames.ContentBodyFrame)
        self.consumers.deliver(tag, msg)
        self.handler.ready()

    @asyncio.coroutine
    def receive_header(self, frame):
        self.synchroniser.notify(frames.ContentHeaderFrame)
        self.message_builder.set_header(frame.payload)
        self.handler.ready()

    @asyncio.coroutine
    def receive_body(self, frame):
        self.message_builder.add_body_chunk(frame.payload)
        if self.message_builder.done():
            msg = self.message_builder.build()
            tag = self.message_builder.consumer_tag
            self.synchroniser.notify(frames.ContentBodyFrame, (tag, msg))
            self.message_builder = None
            # don't call self.handler.ready() if the message is all here -
            # get() or receive_deliver() will call
            # it when they have finished processing the completed msg
            return
        self.handler.ready()


class ChannelMethodSender(bases.Sender):
    def __init__(self, channel_id, protocol, connection_info):
        super().__init__(channel_id, protocol)
        self.connection_info = connection_info

    def send_ChannelOpen(self):
        self.send_method(spec.ChannelOpen(''))

    def send_ExchangeDeclare(self, name, type, durable, auto_delete, internal):
        self.send_method(spec.ExchangeDeclare(0, name, type, False, durable, auto_delete, internal, False, {}))

    def send_ExchangeDelete(self, name, if_unused):
        self.send_method(spec.ExchangeDelete(0, name, if_unused, False))

    def send_QueueDeclare(self, name, durable, exclusive, auto_delete):
        self.send_method(spec.QueueDeclare(0, name, False, durable, exclusive, auto_delete, False, {}))

    def send_QueueBind(self, queue_name, exchange_name, routing_key):
        self.send_method(spec.QueueBind(0, queue_name, exchange_name, routing_key, False, {}))

    def send_QueueUnbind(self, queue_name, exchange_name, routing_key):
        method = spec.QueueUnbind(0, queue_name, exchange_name, routing_key, {})
        self.protocol.send_method(self.channel_id, method)

    def send_QueuePurge(self, queue_name):
        self.send_method(spec.QueuePurge(0, queue_name, False))

    def send_QueueDelete(self, queue_name, if_unused, if_empty):
        self.send_method(spec.QueueDelete(0, queue_name, if_unused, if_empty, False))

    def send_BasicPublish(self, exchange_name, routing_key, mandatory, message):
        self.send_method(spec.BasicPublish(0, exchange_name, routing_key, mandatory, False))
        self.send_content(message)

    def send_BasicConsume(self, queue_name, no_local, no_ack, exclusive):
        self.send_method(spec.BasicConsume(0, queue_name, '', no_local, no_ack, exclusive, False, {}))

    def send_BasicCancel(self, consumer_tag):
        self.send_method(spec.BasicCancel(consumer_tag, False))

    def send_BasicGet(self, queue_name, no_ack):
        self.send_method(spec.BasicGet(0, queue_name, no_ack))

    def send_BasicAck(self, delivery_tag):
        self.send_method(spec.BasicAck(delivery_tag, False))

    def send_BasicReject(self, delivery_tag, redeliver):
        self.send_method(spec.BasicReject(delivery_tag, redeliver))

    def send_Close(self, status_code, msg, class_id, method_id):
        self.send_method(spec.ChannelClose(status_code, msg, class_id, method_id))

    def send_CloseOK(self):
        self.send_method(spec.ChannelCloseOK())

    def send_BasicQos(self, prefetch_size, prefetch_count, apply_globally):
        self.send_method(spec.BasicQos(prefetch_size, prefetch_count, apply_globally))

    def send_content(self, msg):
        header_payload = message.get_header_payload(msg, spec.BasicPublish.method_type[0])
        header_frame = frames.ContentHeaderFrame(self.channel_id, header_payload)
        self.protocol.send_frame(header_frame)

        for payload in message.get_frame_payloads(msg, self.connection_info.frame_max - 8):
            frame = frames.ContentBodyFrame(self.channel_id, payload)
            self.protocol.send_frame(frame)
