import asyncio
from . import spec


class Exchange(object):
    def __init__(self, synchroniser, sender, name, type, durable, auto_delete, internal):
        self.synchroniser = synchroniser
        self.sender = sender
        self.name = name
        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal

    def publish(self, message, routing_key, *, mandatory=True):
        self.sender.send_BasicPublish(self.name, routing_key, mandatory, message)

    @asyncio.coroutine
    def delete(self, *, if_unused=True):
        with (yield from self.synchroniser.sync(spec.ExchangeDeleteOK)) as fut:
            self.sender.send_ExchangeDelete(self.name, if_unused)
            yield from fut
