from ._exceptions import AMQPError
from .spec import EXCEPTIONS, CONSTANTS_INVERSE


__all__ = [
    "AMQPError",
    "ConnectionLostError",
    "UndeliverableMessage",
    "Deleted"
]
__all__.extend(EXCEPTIONS.keys())


class ConnectionLostError(ConnectionError):
    '''
    Connection was closed unexpectedly
    '''
    pass


class UndeliverableMessage(ValueError):
    pass


class Deleted(ValueError):
    pass


globals().update(EXCEPTIONS)


def _get_exception_type(reply_code):
    name = CONSTANTS_INVERSE[reply_code]
    classname = ''.join([x.capitalize() for x in name.split('_')])
    return EXCEPTIONS[classname]
