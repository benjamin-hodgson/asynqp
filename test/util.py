import asyncio
from unittest import mock


def any(cls):
    class _any(cls):
        def __init__(self):
            pass

        def __eq__(self, other):
            return isinstance(other, cls)
    return _any()


from contextlib import contextmanager
@contextmanager
def silence_expected_destroy_pending_log(expected_coro_name=''):
    real_async = asyncio.async
    def async(*args, **kwargs):
        t = real_async(*args, **kwargs)
        if expected_coro_name in repr(t):
            t._log_destroy_pending = False
        return t

    with mock.patch.object(asyncio, 'async', async):
        yield
