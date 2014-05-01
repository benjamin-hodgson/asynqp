import asyncio
from .protocol import AMQP, Dispatcher, HeartbeatMonitor
from .connection import Connection, ConnectionInfo
from .exceptions import AMQPError


@asyncio.coroutine
def connect(host='localhost', port='5672', username='guest', password='guest', virtual_host='/', ssl=None, *, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    dispatcher = Dispatcher()
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher, loop), host=host, port=port, ssl=ssl)
    connection_info = ConnectionInfo(username, password, virtual_host)
    connection = Connection(loop, protocol, dispatcher, connection_info)

    protocol.send_protocol_header()

    yield from dispatcher.handlers[0].opened
    return connection
