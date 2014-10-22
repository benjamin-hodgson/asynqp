import asyncio
import sys
from . import channel
from . import bases
from . import spec
from .util import Synchroniser


class ConnectionInfo(object):
    def __init__(self, username, password, virtual_host):
        self.username = username
        self.password = password
        self.virtual_host = virtual_host


class Connection(object):
    """
    Manage connections to AMQP brokers.

    A :class:`Connection` is a long-lasting mode of communication with a remote server.
    Each connection occupies a single TCP connection, and may carry multiple :class:`Channels <Channel>`.
    A connection communicates with a single virtual host on the server; virtual hosts are
    sandboxed and may not communicate with one another.

    Applications are advised to use one connection for each AMQP peer it needs to communicate with;
    if you need to perform multiple concurrent tasks you should open multiple channels.

    Connections are created using :func:`asynqp.connect() <connect>`.

    .. attribute:: closed

        a :class:`~asyncio.Future` which is done when the handshake to close the connection has finished
    """
    def __init__(self, loop, protocol, synchroniser, sender, dispatcher, connection_info):
        self.synchroniser = synchroniser
        self.sender = sender
        self.channel_factory = channel.ChannelFactory(loop, protocol, dispatcher, connection_info)
        self.connection_info = connection_info

        # this is ugly. when the connection is closing, all methods other than ConnectionCloseOK
        # should be ignored. at the moment this behaviour is part of the dispatcher
        # but this introduces an extra dependency between Connection and ConnectionFrameHandler which
        # i don't like
        self.closing = asyncio.Future()
        self.closing.add_done_callback(lambda fut: dispatcher.closing.set_result(fut.result()))

    @asyncio.coroutine
    def open_channel(self):
        """
        Open a new channel on this connection.

        This method is a :ref:`coroutine <coroutine>`.

        :return: The new :class:`Channel` object.
        """
        channel = yield from self.channel_factory.open()
        return channel

    @asyncio.coroutine
    def close(self):
        """
        Close the connection by handshaking with the server.

        This method is a :ref:`coroutine <coroutine>`.
        """
        self.closing.set_result(True)
        self.sender.send_Close(0, 'Connection closed by application', 0, 0)
        yield from self.synchroniser.await(spec.ConnectionCloseOK)


@asyncio.coroutine
def open_connection(loop, protocol, dispatcher, connection_info):
    synchroniser = Synchroniser()

    sender = ConnectionMethodSender(protocol)
    connection = Connection(loop, protocol, synchroniser, sender, dispatcher, connection_info)
    handler = ConnectionFrameHandler(synchroniser, sender, protocol, connection)

    reader, writer = bases.create_reader_and_writer(handler)

    try:
        dispatcher.add_writer(0, writer)
        protocol.send_protocol_header()
        reader.ready()

        yield from synchroniser.await(spec.ConnectionStart)
        sender.send_StartOK(
            {"product": "asynqp",
             "version": "0.1",  # todo: use pkg_resources to inspect the package
             "platform": sys.version},
            'AMQPLAIN',
            {'LOGIN': connection_info.username, 'PASSWORD': connection_info.password},
            'en_US'
        )
        reader.ready()

        frame = yield from synchroniser.await(spec.ConnectionTune)
        # just agree with whatever the server wants. Make this configurable in future
        connection_info.frame_max = frame.payload.frame_max
        heartbeat_interval = frame.payload.heartbeat
        sender.send_TuneOK(frame.payload.channel_max, frame.payload.frame_max, heartbeat_interval)

        sender.send_Open(connection_info.virtual_host)
        protocol.start_heartbeat(heartbeat_interval)
        reader.ready()

        yield from synchroniser.await(spec.ConnectionOpenOK)
        reader.ready()
    except:
        dispatcher.remove_writer(0)
        raise
    return connection


class ConnectionFrameHandler(bases.FrameHandler):
    def __init__(self, synchroniser, sender, protocol, connection):
        super().__init__(synchroniser, sender)
        self.protocol = protocol
        self.connection = connection

    def handle_ConnectionStart(self, frame):
        self.synchroniser.notify(spec.ConnectionStart)

    def handle_ConnectionTune(self, frame):
        self.synchroniser.notify(spec.ConnectionTune, frame)

    def handle_ConnectionOpenOK(self, frame):
        self.synchroniser.notify(spec.ConnectionOpenOK)

    def handle_ConnectionClose(self, frame):
        self.connection.closing.set_result(True)
        self.sender.send_CloseOK()
        self.protocol.transport.close()

    def handle_ConnectionCloseOK(self, frame):
        self.protocol.transport.close()
        self.synchroniser.notify(spec.ConnectionCloseOK)


class ConnectionMethodSender(bases.Sender):
    def __init__(self, protocol):
        super().__init__(0, protocol)

    def send_StartOK(self, client_properties, mechanism, response, locale):
        method = spec.ConnectionStartOK(client_properties, mechanism, response, locale)
        self.send_method(method)

    def send_TuneOK(self, channel_max, frame_max, heartbeat):
        self.send_method(spec.ConnectionTuneOK(channel_max, frame_max, heartbeat))

    def send_Open(self, virtual_host):
        self.send_method(spec.ConnectionOpen(virtual_host, '', False))

    def send_Close(self, status_code, message, class_id, method_id):
        method = spec.ConnectionClose(status_code, message, class_id, method_id)
        self.send_method(method)

    def send_CloseOK(self):
        self.send_method(spec.ConnectionCloseOK())
