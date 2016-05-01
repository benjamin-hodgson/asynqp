import asyncio
import re

from . import frames
from . import spec
from . import queue
from . import exceptions
from . import exchange
from . import message
from . import routing
from .exceptions import (
    UndeliverableMessage, AMQPError, ChannelClosed)
from .log import log


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
    def __init__(self, id, synchroniser, sender, basic_return_consumer,
                 queue_factory, reader, *, loop):
        self._loop = loop
        self.id = id
        self.synchroniser = synchroniser
        self.sender = sender
        self.basic_return_consumer = basic_return_consumer
        self.queue_factory = queue_factory
        self.reader = reader
        self._closed = False
        # Indicates, that channel is closing by client(!) call
        self._closing = False

    @asyncio.coroutine
    def declare_exchange(self, name, type, *, durable=True, auto_delete=False,
                         passive=False, internal=False, nowait=False,
                         arguments=None):
        """
        Declare an :class:`Exchange` on the broker. If the exchange does not exist, it will be created.

        This method is a :ref:`coroutine <coroutine>`.

        :param str name: the name of the exchange.
        :param str type: the type of the exchange
            (usually one of ``'fanout'``, ``'direct'``, ``'topic'``, or ``'headers'``)
        :keyword bool durable: If true, the exchange will be re-created when
            the server restarts.
        :keyword bool auto_delete: If true, the exchange will be
            deleted when the last queue is un-bound from it.
        :keyword bool passive: If `true` and exchange with such a name does
            not exist it will raise a :class:`exceptions.NotFound`. If `false`
            server will create it. Arguments ``durable``, ``auto_delete`` and
            ``internal`` are ignored if `passive=True`.
        :keyword bool internal: If true, the exchange cannot be published to
            directly; it can only be bound to other exchanges.
        :keyword bool nowait: If true, the method will not wait for declare-ok
            to arrive and return right away.
        :keyword dict arguments: Table of optional parameters for extensions to
            the AMQP protocol. See :ref:`extensions`.

        :return: the new :class:`Exchange` object.
        """
        if name == '':
            return exchange.Exchange(self.reader, self.synchroniser, self.sender, name, 'direct', True, False, False)

        if not VALID_EXCHANGE_NAME_RE.match(name):
            raise ValueError(
                "Invalid exchange name.\n"
                "Valid names consist of letters, digits, hyphen, underscore, "
                "period, or colon, and do not begin with 'amq.'")

        self.sender.send_ExchangeDeclare(
            name, type, passive, durable, auto_delete, internal, nowait,
            arguments or {})
        if not nowait:
            yield from self.synchroniser.await(spec.ExchangeDeclareOK)
            self.reader.ready()
        ex = exchange.Exchange(
            self.reader, self.synchroniser, self.sender, name, type, durable,
            auto_delete, internal)
        return ex

    @asyncio.coroutine
    def declare_queue(self, name='', *, durable=True, exclusive=False,
                      auto_delete=False, passive=False,
                      nowait=False, arguments=None):
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
        :keyword bool passive: If true and queue with such a name does not
            exist it will raise a :class:`exceptions.NotFound` instead of
            creating it. Arguments ``durable``, ``auto_delete`` and
            ``exclusive`` are ignored if ``passive=True``.
        :keyword bool nowait: If true, will not wait for a declare-ok to arrive.
        :keyword dict arguments: Table of optional parameters for extensions to the AMQP protocol. See :ref:`extensions`.

        :return: The new :class:`Queue` object.
        """
        q = yield from self.queue_factory.declare(
            name, durable, exclusive, auto_delete, passive, nowait,
            arguments if arguments is not None else {})
        return q

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
        self.reader.ready()

    def set_return_handler(self, handler):
        """
        Set ``handler`` as the callback function for undeliverable messages
        that were returned by the server.

        By default, an exception is raised, which will be handled by
        the event loop's exception handler (see :meth:`BaseEventLoop.set_exception_handler <asyncio.BaseEventLoop.set_exception_handler>`).
        If ``handler`` is None, this default behaviour is set.

        :param callable handler: A function to be called when a message is returned.
            The callback will be passed the undelivered message.
        """
        self.basic_return_consumer.set_callback(handler)

    def is_closed(self):
        return self._closing or self._closed

    @asyncio.coroutine
    def close(self):
        """
        Close the channel by handshaking with the server.

        This method is a :ref:`coroutine <coroutine>`.
        """
        # If we aren't already closed ask for server to close
        if not self.is_closed():
            self._closing = True
            # Let the ChannelActor do the actual close operations.
            # It will do the work on CloseOK
            self.sender.send_Close(
                0, 'Channel closed by application', 0, 0)
            try:
                yield from self.synchroniser.await(spec.ChannelCloseOK)
            except AMQPError:
                # For example if both sides want to close or the connection
                # is closed.
                pass
        else:
            if self._closing:
                log.warn("Called `close` on already closing channel...")


class ChannelFactory(object):
    def __init__(self, loop, protocol, dispatcher, connection_info):
        self.loop = loop
        self.protocol = protocol
        self.dispatcher = dispatcher
        self.connection_info = connection_info
        self.next_channel_id = 0

    @asyncio.coroutine
    def open(self):
        self.next_channel_id += 1
        channel_id = self.next_channel_id
        synchroniser = routing.Synchroniser(loop=self.loop)

        sender = ChannelMethodSender(channel_id, self.protocol, self.connection_info)
        basic_return_consumer = BasicReturnConsumer(loop=self.loop)
        consumers = queue.Consumers(self.loop)
        consumers.add_consumer(basic_return_consumer)

        actor = ChannelActor(synchroniser, sender, loop=self.loop)
        reader = routing.QueuedReader(actor, loop=self.loop)

        queue_factory = queue.QueueFactory(
            sender, synchroniser, reader, consumers, loop=self.loop)
        channel = Channel(
            channel_id, synchroniser, sender, basic_return_consumer,
            queue_factory, reader, loop=self.loop)

        # Set actor dependencies
        actor.message_receiver = MessageReceiver(synchroniser, sender, consumers, reader)
        actor.consumers = consumers
        actor.channel = channel
        actor.reader = reader

        self.dispatcher.add_handler(channel_id, reader.feed)
        try:
            sender.send_ChannelOpen()
            reader.ready()
            yield from synchroniser.await(spec.ChannelOpenOK)
        except:
            # don't rollback self.next_channel_id;
            # another call may have entered this method
            # concurrently while we were yielding
            # and we don't want to end up with duplicate channels.
            # If we leave self.next_channel_id incremented, the worst
            # that happens is we end up with non-sequential channel numbers.
            # Small price to pay to keep this method re-entrant.
            self.dispatcher.remove_handler(channel_id)
            raise

        reader.ready()
        return channel


class ChannelActor(routing.Actor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Will set those in Channel factory
        self.channel = None
        self.consumers = None
        self.message_receiver = None
        self.reader = None

    def handle(self, frame):
        # From docs on `close`:
        # After sending this method, any received methods except Close and
        # Close-OK MUST be discarded.
        # So we will only process ChannelClose, ChannelCloseOK, PoisonPillFrame
        # if channel is closed
        if self.channel.is_closed():
            close_methods = (spec.ChannelClose, spec.ChannelCloseOK)
            if isinstance(frame.payload, close_methods) or isinstance(frame, frames.PoisonPillFrame):
                return super().handle(frame)
            else:
                return
        return super().handle(frame)

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
        # Send result=None to notify Empty message
        self.synchroniser.notify(spec.BasicGetEmpty, None)

    def handle_BasicConsumeOK(self, frame):
        self.synchroniser.notify(spec.BasicConsumeOK, frame.payload.consumer_tag)

    def handle_BasicCancelOK(self, frame):
        self.synchroniser.notify(spec.BasicCancelOK)

    def handle_BasicQosOK(self, frame):
        self.synchroniser.notify(spec.BasicQosOK)

    def handle_BasicCancel(self, frame):
        # Cancel consumer server-side
        self.consumers.server_cancel(frame.payload.consumer_tag)
        self.reader.ready()

    # Message receiving hanlers

    def handle_BasicGetOK(self, frame):
        assert self.message_receiver is not None, "message_receiver not set"
        # Syncronizer will be notified after full msg is consumed
        self.message_receiver.receive_getOK(frame)

    def handle_BasicDeliver(self, frame):
        assert self.message_receiver is not None, "message_receiver not set"
        self.message_receiver.receive_deliver(frame)

    def handle_ContentHeaderFrame(self, frame):
        assert self.message_receiver is not None, "message_receiver not set"
        self.message_receiver.receive_header(frame)

    def handle_ContentBodyFrame(self, frame):
        assert self.message_receiver is not None, "message_receiver not set"
        self.message_receiver.receive_body(frame)

    def handle_BasicReturn(self, frame):
        assert self.message_receiver is not None, "message_receiver not set"
        self.message_receiver.receive_return(frame)

    # Close handlers

    def handle_PoisonPillFrame(self, frame):
        """ Is sent in case connection was closed or disconnected."""
        self._close_all(frame.exception)

    def handle_ChannelClose(self, frame):
        """ AMQP server closed the channel with an error """
        # By docs:
        # The response to receiving a Close after sending Close must be to
        # send Close-Ok.
        #
        # No need for additional checks

        self.sender.send_CloseOK()
        exc = exceptions._get_exception_type(frame.payload.reply_code)
        self._close_all(exc)

    def handle_ChannelCloseOK(self, frame):
        """ AMQP server closed channel as per our request """
        assert self.channel._closing, "received a not expected CloseOk"
        # Release the `close` method's future
        self.synchroniser.notify(spec.ChannelCloseOK)

        exc = ChannelClosed()
        self._close_all(exc)

    def _close_all(self, exc):
        # Make sure all `close` calls don't deadlock
        self.channel._closed = True
        # If there were anyone who expected an `*-OK` kill them, as no data
        # will follow after close. Any new calls should also raise an error.
        self.synchroniser.killall(exc)
        # Cancel all consumers with same error
        self.consumers.error(exc)


class MessageReceiver(object):
    def __init__(self, synchroniser, sender, consumers, reader):
        self.synchroniser = synchroniser
        self.sender = sender
        self.consumers = consumers
        self.reader = reader
        self.message_builder = None
        self.is_getok_message = None

    def receive_getOK(self, frame):
        payload = frame.payload
        self.message_builder = message.MessageBuilder(
            self.sender,
            payload.delivery_tag,
            payload.redelivered,
            payload.exchange,
            payload.routing_key
        )
        # Send message to synchroniser when done
        self.is_getok_message = True
        self.reader.ready()

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

        # Delivers message to consumers when done
        self.is_getok_message = False
        self.reader.ready()

    def receive_return(self, frame):
        payload = frame.payload
        self.message_builder = message.MessageBuilder(
            self.sender,
            '',
            '',
            payload.exchange,
            payload.routing_key,
            BasicReturnConsumer.tag
        )

        # Delivers message to BasicReturnConsumer when done
        self.is_getok_message = False
        self.reader.ready()

    def receive_header(self, frame):
        assert self.message_builder is not None, "Received unexpected header"
        self.message_builder.set_header(frame.payload)
        if self.message_builder.done():
            self.message_done()
            return
        self.reader.ready()

    def receive_body(self, frame):
        assert self.message_builder is not None, "Received unexpected body"
        self.message_builder.add_body_chunk(frame.payload)

        if self.message_builder.done():
            self.message_done()
            return
        # If message is not done yet we still need more frames. Wait for them
        self.reader.ready()

    def message_done(self):
        msg = self.message_builder.build()
        tag = self.message_builder.consumer_tag
        if self.is_getok_message:
            self.synchroniser.notify(spec.BasicGetOK, (tag, msg))
            # Dont call ready() if message arrive after GetOk. It's the
            # ``Queue.get`` method's responsibility
        else:
            self.consumers.deliver(tag, msg)
            self.reader.ready()

        self.message_builder = None


class ChannelMethodSender(routing.Sender):
    def __init__(self, channel_id, protocol, connection_info):
        super().__init__(channel_id, protocol)
        self.connection_info = connection_info

    def send_ChannelOpen(self):
        self.send_method(spec.ChannelOpen(''))

    def send_ExchangeDeclare(self, name, type, passive, durable, auto_delete, internal, nowait, arguments):
        self.send_method(spec.ExchangeDeclare(0, name, type, passive, durable, auto_delete, internal, nowait, arguments))

    def send_ExchangeDelete(self, name, if_unused):
        self.send_method(spec.ExchangeDelete(0, name, if_unused, False))

    def send_QueueDeclare(self, name, durable, exclusive, auto_delete, passive, nowait, arguments):
        self.send_method(spec.QueueDeclare(0, name, passive, durable, exclusive, auto_delete, nowait, arguments))

    def send_QueueBind(self, queue_name, exchange_name, routing_key, arguments):
        self.send_method(spec.QueueBind(0, queue_name, exchange_name, routing_key, False, arguments))

    def send_QueueUnbind(self, queue_name, exchange_name, routing_key, arguments):
        self.send_method(spec.QueueUnbind(0, queue_name, exchange_name, routing_key, arguments))

    def send_QueuePurge(self, queue_name):
        self.send_method(spec.QueuePurge(0, queue_name, False))

    def send_QueueDelete(self, queue_name, if_unused, if_empty):
        self.send_method(spec.QueueDelete(0, queue_name, if_unused, if_empty, False))

    def send_BasicPublish(self, exchange_name, routing_key, mandatory, message):
        self.send_method(spec.BasicPublish(0, exchange_name, routing_key, mandatory, False))
        self.send_content(message)

    def send_BasicConsume(self, queue_name, no_local, no_ack, exclusive, arguments):
        self.send_method(spec.BasicConsume(0, queue_name, '', no_local, no_ack, exclusive, False, arguments))

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

        for payload in message.get_frame_payloads(msg, self.connection_info['frame_max'] - 8):
            frame = frames.ContentBodyFrame(self.channel_id, payload)
            self.protocol.send_frame(frame)


class BasicReturnConsumer(object):
    tag = -1  # a 'real' tag is a string so there will never be a clash

    def __init__(self, *, loop):
        self.callback = self.default_behaviour
        self.cancelled_future = asyncio.Future(loop=loop)

    def set_callback(self, callback):
        if callback is None:
            self.callback = self.default_behaviour
            return

        if not callable(callback):
            raise TypeError("The handler must be a callable with one argument (a message).", callback)

        self.callback = callback

    def default_behaviour(self, msg):
        raise UndeliverableMessage(msg)
