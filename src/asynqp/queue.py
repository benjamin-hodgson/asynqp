import asyncio
from . import spec
from .exceptions import Deleted


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
    """
    def __init__(self, consumers, synchroniser, loop, sender, name, durable, exclusive, auto_delete):
        self.consumers = consumers
        self.synchroniser = synchroniser
        self.loop = loop
        self.sender = sender
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.deleted = False

    @asyncio.coroutine
    def bind(self, exchange, routing_key):
        """
        Bind a queue to an exchange, with the supplied routing key.

        This action 'subscribes' the queue to the routing key; the precise meaning of this
        varies with the exchange type.

        This method is a :ref:`coroutine <coroutine>`.

        :param asynqp.Exchange exchange: the :class:`Exchange` to bind to
        :param str routing_key: the routing key under which to bind

        :return: The new :class:`QueueBinding` object
        """
        if self.deleted:
            raise Deleted("Queue {} was deleted".format(self.name))

        with (yield from self.synchroniser.sync(spec.QueueBindOK)) as fut:
            self.sender.send_QueueBind(self.name, exchange.name, routing_key)
            yield from fut
            return QueueBinding(self.sender, self.synchroniser, self, exchange, routing_key)

    @asyncio.coroutine
    def consume(self, callback, *, no_local=False, no_ack=False, exclusive=False):
        """
        Start a consumer on the queue. Messages will be delivered asynchronously to the consumer.
        The callback function will be called whenever a new message arrives on the queue.

        This method is a :ref:`coroutine <coroutine>`.

        :param callable callback: a callback to be called when a message is delivered.
            The callback must accept a single argument (an instance of :class:`~asynqp.message.IncomingMessage`).
        :keyword bool no_local: If true, the server will not deliver messages that were
            published by this connection.
        :keyword bool no_ack: If true, messages delivered to the consumer don't require acknowledgement.
        :keyword bool exclusive: If true, only this consumer can access the queue.

        :return: The newly created :class:`Consumer` object.
        """
        if self.deleted:
            raise Deleted("Queue {} was deleted".format(self.name))

        with (yield from self.synchroniser.sync(spec.BasicConsumeOK)) as fut:
            self.sender.send_BasicConsume(self.name, no_local, no_ack, exclusive)
            tag = yield from fut
            consumer = Consumer(tag, callback, self.sender)
            self.consumers.add_consumer(consumer)
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

        with (yield from self.synchroniser.sync(spec.BasicGetOK, spec.BasicGetEmpty)) as fut:
            self.sender.send_BasicGet(self.name, no_ack)
            result = yield from fut
            return result

    @asyncio.coroutine
    def purge(self):
        """
        Purge all undelivered messages from the queue.

        This method is a :ref:`coroutine <coroutine>`.
        """
        with (yield from self.synchroniser.sync(spec.QueuePurgeOK)) as fut:
            self.sender.send_QueuePurge(self.name)
            yield from fut

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

        with (yield from self.synchroniser.sync(spec.QueueDeleteOK)) as fut:
            self.sender.send_QueueDelete(self.name, if_unused, if_empty)
            yield from fut
            self.deleted = True


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
    def __init__(self, sender, synchroniser, queue, exchange, routing_key):
        self.sender = sender
        self.synchroniser = synchroniser
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.deleted = False

    @asyncio.coroutine
    def unbind(self):
        """
        Unbind the queue from the exchange.
        This method is a coroutine.
        """
        if self.deleted:
            raise Deleted("Queue {} was already unbound from exchange {}".format(self.queue.name, self.exchange.name))

        with (yield from self.synchroniser.sync(spec.QueueUnbindOK)) as fut:
            self.sender.send_QueueUnbind(self.queue.name, self.exchange.name, self.routing_key)
            yield from fut
            self.deleted = True


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
    def __init__(self, tag, callback, sender):
        self.tag = tag
        self.callback = callback
        self.sender = sender
        self.cancelled = False

    def cancel(self):
        """
        Cancel the consumer and stop recieving messages.
        """
        self.sender.send_BasicCancel(self.tag)


class Consumers(object):
    def __init__(self, loop):
        self.loop = loop
        self.consumers = {}

    def add_consumer(self, consumer):
        self.consumers[consumer.tag] = consumer

    def deliver(self, tag, msg):
        self.loop.call_soon(self.consumers[tag].callback, msg)

    def cancel(self, tag):
        self.consumers[tag].cancelled = True
        del self.consumers[tag]
