import asyncio
import collections
from . import frames
from . import spec


_TEST = False


class Dispatcher(object):
    def __init__(self):
        self.queue_writers = {}
        self.closing = asyncio.Future()

    def add_writer(self, channel_id, writer):
        self.queue_writers[channel_id] = writer

    def remove_writer(self, channel_id):
        del self.queue_writers[channel_id]

    def dispatch(self, frame):
        if isinstance(frame, frames.HeartbeatFrame):
            return
        if self.closing.done() and not isinstance(frame.payload, (spec.ConnectionClose, spec.ConnectionCloseOK)):
            return
        writer = self.queue_writers[frame.channel_id]
        writer.enqueue(frame)


class Synchroniser(object):
    def __init__(self):
        self._futures = OrderedManyToManyMap()

    def await(self, *expected_methods):
        fut = asyncio.Future()
        self._futures.add_item(expected_methods, fut)
        return fut

    def notify(self, method, result=None):
        fut = self._futures.get_leftmost(method)
        fut.set_result(result)
        self._futures.remove_item(fut)


def create_reader_and_writer(handler):
    q = asyncio.Queue()
    reader = QueueReader(handler, q)
    writer = QueueWriter(q)
    return reader, writer


# When ready() is called, wait for a frame to arrive on the queue.
# When the frame does arrive, dispatch it to the handler and do nothing
# until someone calls ready() again.
class QueueReader(object):
    def __init__(self, handler, q):
        self.handler = handler
        self.q = q
        self.is_waiting = False

    def ready(self):
        assert not self.is_waiting, "ready() got called while waiting for a frame to be read"
        self.is_waiting = True
        t = asyncio.async(self._read_next())
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
        return next(iter(self._map))
