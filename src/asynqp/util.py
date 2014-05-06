import asyncio
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
    def __init__(self, loop, *defaults):
        self.loop = loop
        self.defaults = frozenset(defaults)
        self.expected_methods = self.defaults.copy()
        self.response = None
        self.lock = asyncio.Lock(loop=loop)

    # When yielded from, sync() returns a context manager containing a future.
    # It's not a coroutine; it doesn't make sense to schedule it as a task.
    # You use it like this:
    #
    #     with (yield from s.sync(*expected_methods)) as fut:
    #         ...
    #         yield from fut
    #         ...
    def sync(self, *expected_methods):
        yield from self.lock.acquire()
        self.expected_methods = self.defaults | set(expected_methods)
        self.response = asyncio.Future(loop=self.loop)
        return self.manager(expected_methods)

    def check_expected(self, frame):
        method_type = type(frame.payload)
        expected = self.expected_methods
        if not self.is_expected(frame):
            msg = 'Expected one of {} but got {}'.format([cls.__name__ for cls in expected], method_type.__name__)

            self.fail(AMQPError(msg))
            raise AMQPError(msg)

    def succeed(self, result=None):
        self.response.set_result(result)

    def fail(self, exception):
        self.response.set_exception(exception)

    @contextmanager
    def manager(self, expected_methods):
        try:
            yield self.response
        finally:
            self.expected_methods = self.defaults
            self.response = None
            self.lock.release()

    def is_expected(self, frame):
        method_type = type(frame.payload)
        return (not method_type.synchronous) or (not self.expected_methods) or (method_type in self.expected_methods)
