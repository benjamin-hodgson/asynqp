__all__ = [
    "AMQPError",
    "ConnectionClosedError",
    "ConnectionLostError",
    "UndeliverableMessage",
    "Deleted"
]


class AMQPError(IOError):
    pass


class ConnectionClosedError(ConnectionError):
    '''
    Connection was closed normally by either the amqp server
    or the client.
    '''
    pass


class ConnectionLostError(ConnectionClosedError):
    '''
    Connection was closed unexpectedly
    '''
    pass


class UndeliverableMessage(ValueError):
    pass


class Deleted(ValueError):
    pass
