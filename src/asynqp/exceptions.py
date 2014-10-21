class AMQPError(IOError):
    pass


class UndeliverableMessage(ValueError):
    pass


class Deleted(ValueError):
    pass
