import asyncio
import sys
from . import spec
from . import routing
from . import frames
from .channel import ChannelFactory
from .exceptions import (
    AMQPConnectionError, ConnectionClosed)
from .log import log


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

    .. attribute:: transport

        The :class:`~asyncio.BaseTransport` over which the connection is communicating with the server

    .. attribute:: protocol

        The :class:`~asyncio.Protocol` which is paired with the transport
    """
    def __init__(self, loop, transport, protocol, synchroniser, sender, dispatcher, connection_info):
        self.synchroniser = synchroniser
        self.sender = sender
        self.channel_factory = ChannelFactory(loop, protocol, dispatcher, connection_info)
        self.connection_info = connection_info

        self.transport = transport
        self.protocol = protocol
        # Indicates, that close was initiated by client
        self.closed = asyncio.Future(loop=loop)
        # This future is a backport, so we don't need to log pending errors
        self.closed.add_done_callback(lambda fut: fut.exception())

        self._closing = False

    @asyncio.coroutine
    def open_channel(self):
        """
        Open a new channel on this connection.

        This method is a :ref:`coroutine <coroutine>`.

        :return: The new :class:`Channel` object.
        """
        if self._closing:
            raise ConnectionClosed("Closed by application")
        if self.closed.done():
            raise self.closed.exception()

        channel = yield from self.channel_factory.open()
        return channel

    def is_closed(self):
        " Returns True if connection was closed "
        return self._closing or self.closed.done()

    @asyncio.coroutine
    def close(self):
        """
        Close the connection by handshaking with the server.

        This method is a :ref:`coroutine <coroutine>`.
        """
        if not self.is_closed():
            self._closing = True
            # Let the ConnectionActor do the actual close operations.
            # It will do the work on CloseOK
            self.sender.send_Close(
                0, 'Connection closed by application', 0, 0)
            try:
                yield from self.synchroniser.await(spec.ConnectionCloseOK)
            except AMQPConnectionError:
                # For example if both sides want to close or the connection
                # is closed.
                pass
        else:
            if self._closing:
                log.warn("Called `close` on already closing connection...")
        # finish all pending tasks
        yield from self.protocol.heartbeat_monitor.wait_closed()


@asyncio.coroutine
def open_connection(loop, transport, protocol, dispatcher, connection_info):
    synchroniser = routing.Synchroniser(loop=loop)

    sender = ConnectionMethodSender(protocol)
    connection = Connection(loop, transport, protocol, synchroniser, sender, dispatcher, connection_info)
    actor = ConnectionActor(synchroniser, sender, protocol, connection, dispatcher, loop=loop)
    reader = routing.QueuedReader(actor, loop=loop)

    try:
        dispatcher.add_handler(0, reader.feed)
        protocol.send_protocol_header()
        reader.ready()

        yield from synchroniser.await(spec.ConnectionStart)
        sender.send_StartOK(
            {"product": "asynqp",
             "version": "0.1",  # todo: use pkg_resources to inspect the package
             "platform": sys.version,
             "capabilities": {
                 "consumer_cancel_notify": True
             }},
            'AMQPLAIN',
            {'LOGIN': connection_info['username'], 'PASSWORD': connection_info['password']},
            'en_US'
        )
        reader.ready()

        frame = yield from synchroniser.await(spec.ConnectionTune)
        # just agree with whatever the server wants. Make this configurable in future
        connection_info['frame_max'] = frame.payload.frame_max
        heartbeat_interval = frame.payload.heartbeat
        sender.send_TuneOK(frame.payload.channel_max, frame.payload.frame_max, heartbeat_interval)

        sender.send_Open(connection_info['virtual_host'])
        protocol.start_heartbeat(heartbeat_interval)
        reader.ready()

        yield from synchroniser.await(spec.ConnectionOpenOK)
        reader.ready()
    except:
        dispatcher.remove_handler(0)
        raise
    return connection


class ConnectionActor(routing.Actor):
    def __init__(self, synchroniser, sender, protocol, connection, dispatcher, *, loop=None):
        super().__init__(synchroniser, sender, loop=loop)
        self.protocol = protocol
        self.connection = connection
        self.dispatcher = dispatcher

    def handle(self, frame):
        # From docs on `close`:
        # After sending this method, any received methods except Close and
        # Close-OK MUST be discarded.
        # So we will only process ConnectionClose, ConnectionCloseOK,
        # PoisonPillFrame if channel is closed
        if self.connection.is_closed():
            close_methods = (spec.ConnectionClose, spec.ConnectionCloseOK)
            if isinstance(frame.payload, close_methods) or isinstance(frame, frames.PoisonPillFrame):
                return super().handle(frame)
            else:
                return
        return super().handle(frame)

    def handle_ConnectionStart(self, frame):
        self.synchroniser.notify(spec.ConnectionStart)

    def handle_ConnectionTune(self, frame):
        self.synchroniser.notify(spec.ConnectionTune, frame)

    def handle_ConnectionOpenOK(self, frame):
        self.synchroniser.notify(spec.ConnectionOpenOK)

    # Close handlers

    def handle_PoisonPillFrame(self, frame):
        """ Is sent in case protocol lost connection to server."""
        # Will be delivered after Close or CloseOK handlers. It's for channels,
        # so ignore it.
        if self.connection.closed.done():
            return
        # If connection was not closed already - we lost connection.
        # Protocol should already be closed
        self._close_all(frame.exception)

    def handle_ConnectionClose(self, frame):
        """ AMQP server closed the channel with an error """
        # Notify server we are OK to close.
        self.sender.send_CloseOK()

        exc = ConnectionClosed(frame.payload.reply_text,
                               frame.payload.reply_code)
        self._close_all(exc)
        # This will not abort transport, it will try to flush remaining data
        # asynchronously, as stated in `asyncio` docs.
        self.protocol.close()

    def handle_ConnectionCloseOK(self, frame):
        self.synchroniser.notify(spec.ConnectionCloseOK)
        exc = ConnectionClosed("Closed by application")
        self._close_all(exc)
        # We already agread with server on closing, so lets do it right away
        self.protocol.close()

    def _close_all(self, exc):
        # Make sure all `close` calls don't deadlock
        self.connection.closed.set_exception(exc)
        # Close heartbeat
        self.protocol.heartbeat_monitor.stop()
        # If there were anyone who expected an `*-OK` kill them, as no data
        # will follow after close
        self.synchroniser.killall(exc)
        # Notify all channels about error
        poison_frame = frames.PoisonPillFrame(exc)
        self.dispatcher.dispatch_all(poison_frame)


class ConnectionMethodSender(routing.Sender):
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
