import asyncio
import collections
from . import frames
from .log import log


class Dispatcher(object):
    def __init__(self):
        self.handlers = {}

    def add_handler(self, channel_id, handler):
        self.handlers[channel_id] = handler

    def remove_handler(self, channel_id):
        del self.handlers[channel_id]

    def dispatch(self, frame):
        if isinstance(frame, frames.HeartbeatFrame):
            return
        handler = self.handlers[frame.channel_id]
        handler(frame)

    def dispatch_all(self, frame):
        for handler in self.handlers.values():
            handler(frame)


class Sender(object):
    def __init__(self, channel_id, protocol):
        self.channel_id = channel_id
        self.protocol = protocol

    def send_method(self, method):
        self.protocol.send_method(self.channel_id, method)


class Actor(object):
    def __init__(self, synchroniser, sender, *, loop):
        self._loop = loop
        self.synchroniser = synchroniser
        self.sender = sender

    def handle(self, frame):
        try:
            meth = getattr(self, 'handle_' + type(frame).__name__)
        except AttributeError:
            meth = getattr(self, 'handle_' + type(frame.payload).__name__)

        meth(frame)


class Synchroniser(object):

    def __init__(self, *, loop):
        self._loop = loop
        self._futures = collections.defaultdict(collections.deque)
        self.connection_exc = None

    def await(self, *expected_methods):
        fut = asyncio.Future(loop=self._loop)

        if self.connection_exc is not None:
            fut.set_exception(self.connection_exc)
            return fut

        for method in expected_methods:
            self._futures[method].append(fut)
        return fut

    def notify(self, method, result=None):
        while True:
            try:
                fut = self._futures[method].popleft()
            except IndexError:
                # XXX: we can't just ignore this.
                log.error("Got an unexpected method notification %s", method)
                return
            # We can have done futures if they were awaited together, like
            # (spec.BasicGetOK, spec.BasicGetEmpty).
            if not fut.done():
                break

        fut.set_result(result)

    def killall(self, exc):
        """ Connection/Channel was closed. All subsequent and ongoing requests
            should raise an error
        """
        self.connection_exc = exc
        # Set an exception for all others
        for method, futs in self._futures.items():
            for fut in futs:
                if fut.done():
                    continue
                fut.set_exception(exc)
        self._futures.clear()


# When ready() is called, wait for a frame to arrive on the queue.
# When the frame does arrive, dispatch it to the handler and do nothing
# until someone calls ready() again.
class QueuedReader(object):
    def __init__(self, handler, *, loop):
        self.handler = handler
        self.is_waiting = False
        self.pending_frames = collections.deque()
        self._loop = loop

    def ready(self):
        assert not self.is_waiting, "ready() got called while waiting for a frame to be read"
        if self.pending_frames:
            frame = self.pending_frames.popleft()
            # We will call it in another tick just to be more strict about the
            # sequence of frames
            self._loop.call_soon(self.handler.handle, frame)
        else:
            self.is_waiting = True

    def feed(self, frame):
        if self.is_waiting:
            self.is_waiting = False
            # We will call it in another tick just to be more strict about the
            # sequence of frames
            self._loop.call_soon(self.handler.handle, frame)
        else:
            self.pending_frames.append(frame)
