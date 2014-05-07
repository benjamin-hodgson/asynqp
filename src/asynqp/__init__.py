import asyncio
from . import spec
from .connection import ConnectionInfo, ConnectionFrameHandler
from .exceptions import AMQPError
from .protocol import AMQP, Dispatcher
from .message import Message


@asyncio.coroutine
def connect(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', *, loop=None):
    """
    Connect to an AMQP server on the given host and port.
    Log in to the given virtual host using the supplied credentials.
    This function is a coroutine.

    Arguments:
        host - the host server to connect to.
            default: 'localhost'
        port - the port which the AMQP server is listening on.
            default: 5672
        username - the username to authenticate with.
            default: 'guest'
        password - the password to authenticate with.
            default: 'guest'
        virtual_host - the AMQP virtual host to connect to.
            default: '/'
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    dispatcher = Dispatcher(loop)
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher, loop), host=host, port=port)

    connection_info = ConnectionInfo(username, password, virtual_host)
    handler = ConnectionFrameHandler(protocol, dispatcher, loop, connection_info)
    dispatcher.add_handler(0, handler)

    # fixme: the tests mostly don't go through this function (because they don't want to mock the loop),
    #        so the synchroniser is not in the waiting state when OpenOK arrives. So we can't call synchroniser.succeed().
    #        The design of the code needs to change so that this call to sync() can be done independently
    #        the call to create_connection(). I think pulling a ConnectionController, responsible for creating and coordinating the connection's components,
    #        out of ConnectionFrameHandler would work.
    with (yield from handler.synchroniser.sync(spec.ConnectionStart)) as fut:
        protocol.send_protocol_header()

        yield from handler.opened
        return handler.connection
