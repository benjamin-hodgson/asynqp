from ._exceptions import AMQPError
from .spec import EXCEPTIONS, CONSTANTS_INVERSE


__all__ = [
    "AMQPError",
    "ConnectionClosedError",
    "ConnectionLostError",
    "UndeliverableMessage",
    "Deleted"
]
__all__.extend(EXCEPTIONS.keys())


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


globals().update(EXCEPTIONS)


def get_exception_type(reply_code):
    name = CONSTANTS_INVERSE[reply_code]
    classname = ''.join([x.capitalize() for x in name.split('_')])
    return EXCEPTIONS[classname]
