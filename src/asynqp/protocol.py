import asyncio
import struct
from . import spec
from . import frames
from .exceptions import AMQPError, ConnectionLostError
from .log import log


class AMQP(asyncio.Protocol):
    def __init__(self, dispatcher, loop):
        self.dispatcher = dispatcher
        self.partial_frame = b''
        self.frame_reader = FrameReader()
        self.heartbeat_monitor = HeartbeatMonitor(self, loop)
        self._closed = False

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        while data:
            self.heartbeat_monitor.heartbeat_received()  # the spec says 'any octet may substitute for a heartbeat'

            try:
                result = self.frame_reader.read_frame(data)
            except AMQPError:
                self.close()
                raise

            if result is None:  # incomplete frame, wait for the rest
                return
            frame, remainder = result

            self.dispatcher.dispatch(frame)
            data = remainder

    def send_method(self, channel, method):
        frame = frames.MethodFrame(channel, method)
        self.send_frame(frame)

    def send_frame(self, frame):
        self.transport.write(frame.serialise())

    def send_protocol_header(self):
        self.transport.write(b'AMQP\x00\x00\x09\x01')

    def start_heartbeat(self, heartbeat_interval):
        self.heartbeat_monitor.start(heartbeat_interval)

    def connection_lost(self, exc):
        # If self._closed=True - we closed the transport ourselves. No need to
        # dispatch PoisonPillFrame, as we should have closed everything already
        if not self._closed:
            poison_exc = ConnectionLostError(
                'The connection was unexpectedly lost', exc)
            self.dispatcher.dispatch_all(frames.PoisonPillFrame(poison_exc))
            # XXX: Really do we even need to raise this??? It's super bad API
            raise poison_exc from exc

    def heartbeat_timeout(self):
        """ Called by heartbeat_monitor on timeout """
        assert not self._closed, "Did we not stop heartbeat_monitor on close?"
        log.error("Heartbeat time out")
        poison_exc = ConnectionLostError('Heartbeat timed out')
        poison_frame = frames.PoisonPillFrame(poison_exc)
        self.dispatcher.dispatch_all(poison_frame)
        # Spec says to just close socket without ConnectionClose handshake.
        self.close()

    def close(self):
        assert not self._closed, "Why do we close it 2-ce?"
        self._closed = True
        self.transport.close()


class FrameReader(object):
    def __init__(self):
        self.partial_frame = b''

    def read_frame(self, data):
        data = self.partial_frame + data
        self.partial_frame = b''

        if len(data) < 7:
            self.partial_frame = data
            return

        frame_header = data[:7]
        frame_type, channel_id, size = struct.unpack('!BHL', frame_header)

        if len(data) < size + 8:
            self.partial_frame = data
            return

        raw_payload = data[7:7 + size]
        frame_end = data[7 + size]

        if frame_end != spec.FRAME_END:
            raise AMQPError("Frame end byte was incorrect")

        frame = frames.read(frame_type, channel_id, raw_payload)
        remainder = data[8 + size:]

        return frame, remainder


class HeartbeatMonitor(object):
    def __init__(self, protocol, loop):
        self.protocol = protocol
        self.loop = loop
        self.send_hb_task = None
        self.monitor_task = None
        self._last_received = 0

    def start(self, interval):
        if interval <= 0:
            return
        self.send_hb_task = asyncio.async(self.send_heartbeat(interval), loop=self.loop)
        self.monitor_task = asyncio.async(self.monitor_heartbeat(interval), loop=self.loop)

    def stop(self):
        if self.send_hb_task is not None:
            self.send_hb_task.cancel()
        if self.monitor_task is not None:
            self.monitor_task.cancel()

    @asyncio.coroutine
    def wait_closed(self):
        if self.send_hb_task is not None:
            try:
                yield from self.send_hb_task
            except asyncio.CancelledError:
                pass
        if self.monitor_task is not None:
            try:
                yield from self.monitor_task
            except asyncio.CancelledError:
                pass

    @asyncio.coroutine
    def send_heartbeat(self, interval):
        # XXX: Add `last_sent` frame monitoring to not send heartbeats
        #      if traffic was going through socket
        while True:
            self.protocol.send_frame(frames.HeartbeatFrame())
            yield from asyncio.sleep(interval, loop=self.loop)

    @asyncio.coroutine
    def monitor_heartbeat(self, interval):
        self._last_received = self.loop.time()
        no_beat_for = 0
        while True:
            # As spec states:
            # If a peer detects no incoming traffic (i.e. received octets) for
            # two heartbeat intervals or longer, it should close the connection
            yield from asyncio.sleep(
                interval * 2 - no_beat_for, loop=self.loop)

            no_beat_for = self.loop.time() - self._last_received
            if no_beat_for > interval * 2:
                self.protocol.heartbeat_timeout()
                self.send_hb_task.cancel()
                return

    def heartbeat_received(self):
        self._last_received = self.loop.time()
