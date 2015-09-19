import asyncio
import collections
from . import frames
from . import spec


_TEST = False


class Dispatcher(object):
    def __init__(self):
        self.queue_writers = {}

    def add_writer(self, channel_id, writer):
        self.queue_writers[channel_id] = writer

    def remove_writer(self, channel_id):
        del self.queue_writers[channel_id]

    def dispatch(self, frame):
        if isinstance(frame, frames.HeartbeatFrame):
            return
        writer = self.queue_writers[frame.channel_id]
        writer.enqueue(frame)

    def dispatch_all(self, frame):
        for writer in self.queue_writers.values():
            writer.enqueue(frame)


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
        self.closing = asyncio.Future(loop=self._loop)

    def handle(self, frame):
        if self.closing.done() and not isinstance(frame.payload, (spec.ConnectionClose, spec.ConnectionCloseOK)):
            return
        try:
            meth = getattr(self, 'handle_' + type(frame).__name__)
        except AttributeError:
            meth = getattr(self, 'handle_' + type(frame.payload).__name__)

        meth(frame)

    def handle_PoisonPillFrame(self, frame):
        self.synchroniser.killall(ConnectionError)


class Synchroniser(object):
    _blocking_methods = set((spec.BasicCancelOK,  # Consumer.cancel
                             spec.ChannelCloseOK,  # Channel.close
                             spec.ConnectionCloseOK))  # Connection.close

    def __init__(self, *, loop):
        self._loop = loop
        self._futures = OrderedManyToManyMap()
        self.connection_closed = False

    def await(self, *expected_methods):
        fut = asyncio.Future(loop=self._loop)

        if self.connection_closed:
            for method in expected_methods:
                if method in self._blocking_methods and not fut.done():
                    fut.set_result(None)
            if not fut.done():
                fut.set_exception(ConnectionError)
            return fut

        self._futures.add_item(expected_methods, fut)
        return fut

    def notify(self, method, result=None):
        fut = self._futures.get_leftmost(method)
        fut.set_result(result)
        self._futures.remove_item(fut)

    def killall(self, exc):
        self.connection_closed = True
        # Give a proper notification to methods which are waiting for closure
        for method in self._blocking_methods:
            while True:
                try:
                    self.notify(method)
                except StopIteration:
                    break

        # Set an exception for all others
        for method in self._futures.keys():
            if method not in self._blocking_methods:
                for fut in self._futures.get_all(method):
                    fut.set_exception(exc)
                    self._futures.remove_item(fut)


def create_reader_and_writer(handler, *, loop):
    q = asyncio.Queue(loop=loop)
    reader = QueueReader(handler, q, loop=loop)
    writer = QueueWriter(q)
    return reader, writer


# When ready() is called, wait for a frame to arrive on the queue.
# When the frame does arrive, dispatch it to the handler and do nothing
# until someone calls ready() again.
class QueueReader(object):
    def __init__(self, handler, q, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self.handler = handler
        self.q = q
        self.is_waiting = False

    def ready(self):
        assert not self.is_waiting, "ready() got called while waiting for a frame to be read"
        self.is_waiting = True

        # XXX: Refactor this. There should be only 1 async task per QueueReader
        #      It will read frames in a `while True:` loop and will be canceled
        #      when connection is closed.

        t = asyncio.async(self._read_next(), loop=self._loop)
        if _TEST:  # this feels hacky to me
            t._log_destroy_pending = False

    @asyncio.coroutine
    def _read_next(self):
        assert self.is_waiting, "a frame got read without ready() having been called"
        frame = yield from self.q.get()
        self.is_waiting = False
        self.handler.handle(frame)


class QueueWriter(object):
    def __init__(self, q):
        self.q = q

    def enqueue(self, frame):
        self.q.put_nowait(frame)


class OrderedManyToManyMap(object):
    def __init__(self):
        self._items = collections.defaultdict(OrderedSet)

    def add_item(self, keys, item):
        for key in keys:
            self._items[key].add(item)

    def remove_item(self, item):
        for ordered_set in self._items.values():
            ordered_set.discard(item)

    def get_leftmost(self, key):
        return self._items[key].first()

    def get_all(self, key):
        return list(self._items[key])

    def keys(self):
        return (k for k, v in self._items.items() if v)


class OrderedSet(collections.MutableSet):
    def __init__(self):
        self._map = collections.OrderedDict()

    def __contains__(self, item):
        return item in self._map

    def __iter__(self):
        return iter(self._map.keys())

    def __getitem__(self, ix):
        return

    def __len__(self):
        return len(self._map)

    def add(self, item):
        self._map[item] = None

    def discard(self, item):
        try:
            del self._map[item]
        except KeyError:
            pass

    def first(self):
        return next(iter(self))
