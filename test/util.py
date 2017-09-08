import asyncio
from contextlib import contextmanager
from unittest import mock
import asynqp.frames
from asynqp import protocol
from asynqp.exceptions import ConnectionLostError


def testing_exception_handler(loop, context):
    '''
    Hides the expected ``ConnectionClosedErrors`` and
    ``ConnectionLostErros`` during tests
    '''
    exception = context.get('exception')
    if exception and isinstance(exception, ConnectionLostError):
        pass
    else:
        loop.default_exception_handler(context)


class MockServer(object):
    def __init__(self, protocol, tick):
        self.protocol = protocol
        self.tick = tick
        self.data = []

    def send_bytes(self, b):
        self.protocol.data_received(b)
        self.tick()

    def send_frame(self, frame):
        self.send_bytes(frame.serialise())

    def send_method(self, channel_number, method):
        frame = asynqp.frames.MethodFrame(channel_number, method)
        self.send_frame(frame)

    def reset(self):
        self.data = []

    def should_have_received_frames(self, expected_frames, any_order=False):
        results = (read(x) for x in self.data)
        frames = [x for x in results if x is not None]
        if any_order:
            for frame in expected_frames:
                assert frame in frames, "{} should have been in {}".format(frame, frames)
        else:
            expected_frames = tuple(expected_frames)
            assert expected_frames in windows(frames, len(expected_frames)), "{} should have been in {}".format(expected_frames, frames)

    def should_have_received_methods(self, channel_number, methods, any_order=False):
        frames = (asynqp.frames.MethodFrame(channel_number, m) for m in methods)
        self.should_have_received_frames(frames, any_order)

    def should_have_received_frame(self, expected_frame):
        self.should_have_received_frames([expected_frame], any_order=True)

    def should_have_received_method(self, channel_number, method):
        self.should_have_received_methods(channel_number, [method], any_order=True)

    def should_not_have_received_method(self, channel_number, method):
        results = (read(x) for x in self.data)
        frames = [x for x in results if x is not None]

        frame = asynqp.frames.MethodFrame(channel_number, method)
        assert frame not in frames, "{} should not have been in {}".format(frame, frames)

    def should_not_have_received_any(self):
        assert not self.data, "{} should have been empty".format(self.data)

    def should_have_received_bytes(self, b):
        assert b in self.data


def read(data):
    if data == b'AMQP\x00\x00\x09\x01':
        return

    result = protocol.FrameReader().read_frame(data)
    if result is None:
        return
    return result[0]


def windows(l, size):
    return zip(*[l[x:] for x in range(size)])


class FakeTransport(object):
    def __init__(self, server):
        self.server = server
        self.closed = False

    def write(self, data):
        self.server.data.append(data)

    def close(self):
        self.closed = True


def any(cls):
    class _any(cls):
        def __init__(self):
            pass

        def __eq__(self, other):
            return isinstance(other, cls)
    return _any()


@contextmanager
def silence_expected_destroy_pending_log(expected_coro_name=''):
    real_async = asyncio.ensure_future

    def async(*args, **kwargs):
        t = real_async(*args, **kwargs)
        if expected_coro_name in repr(t):
            t._log_destroy_pending = False
        return t

    with mock.patch.object(asyncio, 'async', async):
        yield
