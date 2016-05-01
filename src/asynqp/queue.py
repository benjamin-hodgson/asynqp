import asyncio
import re
from . import spec
from .exceptions import Deleted, AMQPError


VALID_QUEUE_NAME_RE = re.compile(r'^(?!amq\.)(\w|[-.:])*$', flags=re.A)


class Queue(object):
    """
    Manage AMQP Queues and consume messages.

    A queue is a collection of messages, to which new messages can be delivered via an :class:`Exchange`,
    and from which messages can be consumed by an application.

    Queues are created using :meth:`Channel.declare_queue() <Channel.declare_queue>`.

    .. attribute:: name

        the name of the queue

    .. attribute:: durable

        if True, the queue will be re-created when the broker restarts

    .. attribute:: exclusive

        if True, the queue is only accessible over one channel

    .. attribute:: auto_delete

        if True, the queue will be deleted when its last consumer is removed

    .. attribute:: arguments

        A dictionary of the extra arguments that were used to declare the queue.
    """
    def __init__(self, reader, consumers, synchroniser, sender, name, durable, exclusive, auto_delete, arguments, *, loop):
        self._loop = loop
        self.reader = reader
        self.consumers = consumers
        self.synchroniser = synchroniser
        self.sender = sender

        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.deleted = False

    @asyncio.coroutine
    def bind(self, exchange, routing_key, *, arguments=None):
        """
        Bind a queue to an exchange, with the supplied routing key.

        This action 'subscribes' the queue to the routing key; the precise meaning of this
        varies with the exchange type.

        This method is a :ref:`coroutine <coroutine>`.

        :param asynqp.Exchange exchange: the :class:`Exchange` to bind to
        :param str routing_key: the routing key under which to bind
        :keyword dict arguments: Table of optional parameters for extensions to the AMQP protocol. See :ref:`extensions`.

        :return: The new :class:`QueueBinding` object
        """
        if self.deleted:
            raise Deleted("Queue {} was deleted".format(self.name))

        self.sender.send_QueueBind(self.name, exchange.name, routing_key, arguments or {})
        yield from self.synchroniser.await(spec.QueueBindOK)
        b = QueueBinding(self.reader, self.sender, self.synchroniser, self, exchange, routing_key)
        self.reader.ready()
        return b

    @asyncio.coroutine
    def consume(self, callback, *, no_local=False, no_ack=False, exclusive=False, arguments=None):
        """
        Start a consumer on the queue. Messages will be delivered asynchronously to the consumer.
        The callback function will be called whenever a new message arrives on the queue.

        Advanced usage: the callback object must be callable
        (it must be a function or define a ``__call__`` method),
        but may also define some further methods:

        * ``callback.on_cancel()``: called with no parameters when the consumer is successfully cancelled.
        * ``callback.on_error(exc)``: called when the channel is closed due to an error.
          The argument passed is the exception which caused the error.

        This method is a :ref:`coroutine <coroutine>`.

        :param callable callback: a callback to be called when a message is delivered.
            The callback must accept a single argument (an instance of :class:`~asynqp.message.IncomingMessage`).
        :keyword bool no_local: If true, the server will not deliver messages that were
            published by this connection.
        :keyword bool no_ack: If true, messages delivered to the consumer don't require acknowledgement.
        :keyword bool exclusive: If true, only this consumer can access the queue.
        :keyword dict arguments: Table of optional parameters for extensions to the AMQP protocol. See :ref:`extensions`.

        :return: The newly created :class:`Consumer` object.
        """
        if self.deleted:
            raise Deleted("Queue {} was deleted".format(self.name))

        self.sender.send_BasicConsume(self.name, no_local, no_ack, exclusive, arguments or {})
        tag = yield from self.synchroniser.await(spec.BasicConsumeOK)
        consumer = Consumer(
            tag, callback, self.sender, self.synchroniser, self.reader,
            loop=self._loop)
        self.consumers.add_consumer(consumer)
        self.reader.ready()
        return consumer

    @asyncio.coroutine
    def get(self, *, no_ack=False):
        """
        Synchronously get a message from the queue.

        This method is a :ref:`coroutine <coroutine>`.

        :keyword bool no_ack: if true, the broker does not require acknowledgement of receipt of the message.

        :return: an :class:`~asynqp.message.IncomingMessage`,
            or ``None`` if there were no messages on the queue.
        """
        if self.deleted:
            raise Deleted("Queue {} was deleted".format(self.name))

        self.sender.send_BasicGet(self.name, no_ack)
        tag_msg = yield from self.synchroniser.await(spec.BasicGetOK, spec.BasicGetEmpty)

        if tag_msg is not None:
            consumer_tag, msg = tag_msg
            assert consumer_tag is None
        else:
            msg = None
        self.reader.ready()
        return msg

    @asyncio.coroutine
    def purge(self):
        """
        Purge all undelivered messages from the queue.

        This method is a :ref:`coroutine <coroutine>`.
        """
        self.sender.send_QueuePurge(self.name)
        yield from self.synchroniser.await(spec.QueuePurgeOK)
        self.reader.ready()

    @asyncio.coroutine
    def delete(self, *, if_unused=True, if_empty=True):
        """
        Delete the queue.

        This method is a :ref:`coroutine <coroutine>`.

        :keyword bool if_unused: If true, the queue will only be deleted
            if it has no consumers.
        :keyword bool if_empty: If true, the queue will only be deleted if
            it has no unacknowledged messages.
        """
        if self.deleted:
            raise Deleted("Queue {} was already deleted".format(self.name))

        self.sender.send_QueueDelete(self.name, if_unused, if_empty)
        yield from self.synchroniser.await(spec.QueueDeleteOK)
        self.deleted = True
        self.reader.ready()


class QueueBinding(object):
    """
    Manage queue-exchange bindings.

    Represents a binding between a :class:`Queue` and an :class:`Exchange`.
    Once a queue has been bound to an exchange, messages published
    to that exchange will be delivered to the queue. The delivery
    may be conditional, depending on the type of the exchange.

    QueueBindings are created using :meth:`Queue.bind() <Queue.bind>`.

    .. attribute:: queue

        the :class:`Queue` which was bound

    .. attribute:: exchange

        the :class:`Exchange` to which the queue was bound

    .. attribute:: routing_key

        the routing key used for the binding
    """
    def __init__(self, reader, sender, synchroniser, queue, exchange, routing_key):
        self.reader = reader
        self.sender = sender
        self.synchroniser = synchroniser
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.deleted = False

    @asyncio.coroutine
    def unbind(self, arguments=None):
        """
        Unbind the queue from the exchange.

        This method is a :ref:`coroutine <coroutine>`.
        """
        if self.deleted:
            raise Deleted("Queue {} was already unbound from exchange {}".format(self.queue.name, self.exchange.name))

        self.sender.send_QueueUnbind(self.queue.name, self.exchange.name, self.routing_key, arguments or {})
        yield from self.synchroniser.await(spec.QueueUnbindOK)
        self.deleted = True
        self.reader.ready()


class Consumer(object):
    """
    A consumer asynchronously recieves messages from a queue as they arrive.

    Consumers are created using :meth:`Queue.consume() <Queue.consume>`.

    .. attribute :: tag

        A string representing the *consumer tag* used by the server to identify this consumer.

    .. attribute :: callback

        The callback function that is called when messages are delivered to the consumer.
        This is the function that was passed to :meth:`Queue.consume() <Queue.consume>`,
        and should accept a single :class:`~asynqp.message.IncomingMessage` argument.

    .. attribute :: cancelled

        Boolean. True if the consumer has been successfully cancelled.
    """
    def __init__(self, tag, callback, sender, synchroniser, reader, *, loop):
        self._loop = loop
        self.tag = tag
        self.callback = callback
        self.sender = sender
        self.cancelled = False
        self.synchroniser = synchroniser
        self.reader = reader
        self.cancelled_future = asyncio.Future(loop=self._loop)

    @asyncio.coroutine
    def cancel(self):
        """
        Cancel the consumer and stop recieving messages.

        This method is a :ref:`coroutine <coroutine>`.
        """
        self.sender.send_BasicCancel(self.tag)
        try:
            yield from self.synchroniser.await(spec.BasicCancelOK)
        except AMQPError:
            pass
        else:
            # No need to call ready if channel closed.
            self.reader.ready()
        self.cancelled = True
        self.cancelled_future.set_result(self)
        if hasattr(self.callback, 'on_cancel'):
            self.callback.on_cancel()


class QueueFactory(object):
    def __init__(self, sender, synchroniser, reader, consumers, *, loop):
        self._loop = loop
        self.sender = sender
        self.synchroniser = synchroniser
        self.reader = reader
        self.consumers = consumers

    @asyncio.coroutine
    def declare(self, name, durable, exclusive, auto_delete, passive, nowait,
                arguments):
        if not VALID_QUEUE_NAME_RE.match(name):
            raise ValueError(
                "Not a valid queue name.\n"
                "Valid names consist of letters, digits, hyphen, underscore, "
                "period, or colon, and do not begin with 'amq.'")
        if not name and nowait:
            raise ValueError("Declaring the queue without `name` and with "
                             "`nowait` is forbidden")

        self.sender.send_QueueDeclare(
            name, durable, exclusive, auto_delete, passive, nowait, arguments)
        if not nowait:
            name = yield from self.synchroniser.await(spec.QueueDeclareOK)
            self.reader.ready()
        q = Queue(self.reader, self.consumers, self.synchroniser, self.sender,
                  name, durable, exclusive, auto_delete, arguments,
                  loop=self._loop)
        return q


class Consumers(object):
    def __init__(self, loop):
        self.loop = loop
        self.consumers = {}

    def add_consumer(self, consumer):
        self.consumers[consumer.tag] = consumer
        # so the consumer gets garbage collected when it is cancelled
        consumer.cancelled_future.add_done_callback(lambda fut: self.consumers.pop(fut.result().tag, None))

    def deliver(self, tag, msg):
        assert tag in self.consumers, "Message got delivered to a non existent consumer"
        consumer = self.consumers[tag]
        self.loop.call_soon(consumer.callback, msg)

    def server_cancel(self, tag):
        consumer = self.consumers.pop(tag, None)
        if consumer:
            if hasattr(consumer.callback, 'on_cancel'):
                consumer.callback.on_cancel()

    def error(self, exc):
        for consumer in self.consumers.values():
            if hasattr(consumer.callback, 'on_error'):
                consumer.callback.on_error(exc)
