import asyncio
from . import spec


class Exchange(object):
    """
    Manage AMQP Exchanges and publish messages.

    An exchange is a 'routing node' to which messages can be published.
    When a message is published to an exchange, the exchange determines which :class:`Queue`
    to deliver the message to by inspecting the message's routing key and the exchange's bindings.
    You can bind a queue to an exchange, to start receiving messages on the queue,
    using :meth:`Queue.bind`.

    Exchanges are created using :meth:`Channel.declare_exchange() <Channel.declare_exchange>`.

    .. attribute:: name

        the name of the exchange.

    .. attribute:: type

        the type of the exchange (usually one of ``'fanout'``, ``'direct'``, ``'topic'``, or ``'headers'``).
    """
    def __init__(self, reader, synchroniser, sender, name, type, durable, auto_delete, internal):
        self.reader = reader
        self.synchroniser = synchroniser
        self.sender = sender
        self.name = name
        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal

    def publish(self, message, routing_key, *, mandatory=True):
        """
        Publish a message on the exchange, to be asynchronously delivered to queues.

        :param asynqp.Message message: the message to send
        :param str routing_key: the routing key with which to publish the message
        """
        self.sender.send_BasicPublish(self.name, routing_key, mandatory, message)

    @asyncio.coroutine
    def delete(self, *, if_unused=True):
        """
        Delete the exchange.

        This method is a :ref:`coroutine <coroutine>`.

        :keyword bool if_unused: If true, the exchange will only be deleted if
            it has no queues bound to it.
        """
        self.sender.send_ExchangeDelete(self.name, if_unused)
        yield from self.synchroniser.await(spec.ExchangeDeleteOK)
        self.reader.ready()
