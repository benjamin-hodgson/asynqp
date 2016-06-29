# flake8: noqa

import socket
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

    This function will set TCP_NODELAY on TCP and TCP6 sockets either on supplied ``sock`` or created one.

    :return: the :class:`Connection` object.
    """
    from .protocol import AMQP
    from .routing import Dispatcher
    from .connection import open_connection

    loop = asyncio.get_event_loop() if loop is None else loop

    if sock is None:
        kwargs['host'] = host
        kwargs['port'] = port
    else:
        kwargs['sock'] = sock

    dispatcher = Dispatcher()
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher, loop), **kwargs)

    # RPC-like applications require TCP_NODELAY in order to acheive
    # minimal response time. Actually, this library send data in one
    # big chunk and so this will not affect TCP-performance.
    sk = transport.get_extra_info('socket')
    # 1. Unfortunatelly we cannot check socket type (sk.type == socket.SOCK_STREAM). https://bugs.python.org/issue21327
    # 2. Proto remains zero, if not specified at creation of socket
    if (sk.family in (socket.AF_INET, socket.AF_INET6)) and (sk.proto in (0, socket.IPPROTO_TCP)):
        sk.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    connection_info = {
        'username': username,
        'password': password,
        'virtual_host': virtual_host
    }
    connection = yield from open_connection(
        loop, transport, protocol, dispatcher, connection_info)
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
