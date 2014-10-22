import asyncio
import collections
from contextlib import contextmanager
from .exceptions import AMQPError


def rethrow_as(expected_cls, to_throw):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except expected_cls as e:
                raise to_throw from e
        return wrapper
    return decorator


class Synchroniser(object):
    def __init__(self):
        self.futures = OrderedManyToManyMap()

    def await(self, *expected_methods):
        fut = asyncio.Future()
        self.futures.add_item(expected_methods, fut)
        return fut

    def notify(self, method, result=None):
        fut = self.futures.get_leftmost(method)
        fut.set_result(result)
        self.futures.remove_item(fut)


class OrderedManyToManyMap(object):
    def __init__(self):
        self.items = collections.defaultdict(OrderedSet)

    def add_item(self, keys, item):
        for key in keys:
            self.items[key].add(item)

    def remove_item(self, item):
        for ordered_set in self.items.values():
            ordered_set.discard(item)

    def get_leftmost(self, key):
        return self.items[key].first()


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
