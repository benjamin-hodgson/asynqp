from ._exceptions import AMQPError, AMQPChannelError
from .spec import EXCEPTIONS, CONSTANTS_INVERSE


__all__ = [
    "AMQPError",
    "ConnectionLostError",
    "ChannelClosed",
    "ConnectionClosed",
    "AMQPChannelError",
    "AMQPConnectionError",
    "UndeliverableMessage",
    "Deleted"
]
__all__.extend(EXCEPTIONS.keys())


class AMQPConnectionError(AMQPError):
    pass


class ConnectionLostError(AMQPConnectionError, ConnectionError):
    """ Connection was closed unexpectedly """

    def __init__(self, message, exc=None):
        super().__init__(message)
        self.original_exc = exc


class ConnectionClosed(AMQPConnectionError):
    """ Connection was closed by client """

    def __init__(self, reply_text, reply_code=None):
        super().__init__(reply_text)
        self.reply_text = reply_text
        self.reply_code = reply_code


class ChannelClosed(AMQPChannelError):
    """ Channel was closed by client """


class UndeliverableMessage(ValueError):
    pass


class Deleted(ValueError):
    pass


globals().update(EXCEPTIONS)


def _get_exception_type(reply_code):
    name = CONSTANTS_INVERSE[reply_code]
    classname = ''.join([x.capitalize() for x in name.split('_')])
    return EXCEPTIONS[classname]
