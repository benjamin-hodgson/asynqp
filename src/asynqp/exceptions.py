__all__ = [
    "AMQPError",
    "UndeliverableMessage",
    "Deleted"
]


class AMQPError(IOError):
    pass


class UndeliverableMessage(ValueError):
    pass


class Deleted(ValueError):
    pass
