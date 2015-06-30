import asyncio
from .exceptions import *  # noqa
from .message import Message, IncomingMessage
from .connection import Connection
from .channel import Channel
from .exchange import Exchange
from .queue import Queue, QueueBinding, Consumer


__all__ = [
    "Message", "IncomingMessage",
    "Connection", "Channel", "Exchange", "Queue", "QueueBinding", "Consumer",
    "connect", "connect_and_open_channel"
]
__all__ += exceptions.__all__


@asyncio.coroutine
def connect(host='localhost',
            port=5672,
            username='guest', password='guest',
            virtual_host='/', *,
            loop=None, sock=None, **kwargs):
    """
    Connect to an AMQP server on the given host and port.

    Log in to the given virtual host using the supplied credentials.
    This function is a :ref:`coroutine <coroutine>`.

    :param str host: the host server to connect to.
    :param int port: the port which the AMQP server is listening on.
    :param str username: the username to authenticate with.
    :param str password: the password to authenticate with.
    :param str virtual_host: the AMQP virtual host to connect to.
    :keyword BaseEventLoop loop: An instance of :class:`~asyncio.BaseEventLoop` to use.
        (Defaults to :func:`asyncio.get_event_loop()`)
    :keyword socket sock: A :func:`~socket.socket` instance to use for the connection.
        This is passed on to :meth:`loop.create_connection() <asyncio.BaseEventLoop.create_connection>`.
        If ``sock`` is supplied then ``host`` and ``port`` will be ignored.

    Further keyword arguments are passed on to :meth:`loop.create_connection() <asyncio.BaseEventLoop.create_connection>`.

    :return: the :class:`Connection` object.
    """
    from .protocol import AMQP
    from .routing import Dispatcher
    from .connection import ConnectionInfo, open_connection

    loop = asyncio.get_event_loop() if loop is None else loop

    if sock is None:
        kwargs['host'] = host
        kwargs['port'] = port
    else:
        kwargs['sock'] = sock

    dispatcher = Dispatcher()
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher, loop), **kwargs)

    connection = yield from open_connection(loop, transport, protocol, dispatcher, ConnectionInfo(username, password, virtual_host))
    return connection


@asyncio.coroutine
def connect_and_open_channel(host='localhost',
                             port=5672,
                             username='guest', password='guest',
                             virtual_host='/', *,
                             loop=None, **kwargs):
    """
    Connect to an AMQP server and open a channel on the connection.
    This function is a :ref:`coroutine <coroutine>`.

    Parameters of this function are the same as :func:`connect`.

    :return: a tuple of ``(connection, channel)``.

    Equivalent to::

        connection = yield from connect(host, port, username, password, virtual_host, loop=loop, **kwargs)
        channel = yield from connection.open_channel()
        return connection, channel
    """
    connection = yield from connect(host, port, username, password, virtual_host, loop=loop, **kwargs)
    channel = yield from connection.open_channel()
    return connection, channel
