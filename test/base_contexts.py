import asyncio
import asynqp
import asynqp.bases
from asyncio import test_utils
from asynqp import spec
from asynqp import protocol
from asynqp.connection import ConnectionInfo, open_connection
from unittest import mock
from .util import MockServer, FakeTransport


class LoopContext:
    def given_an_event_loop(self):
        self.loop = asyncio.get_event_loop()
        asynqp.bases._TEST = True

    def tick(self):
        test_utils.run_briefly(self.loop)

    def cleanup_test_hack(self):
        asynqp.bases._TEST = False

    def async_partial(self, coro):
        """
        Schedule a coroutine which you are not expecting to complete before the end of the test.
        Disables the error log when the task is destroyed before completing.
        """
        t = asyncio.async(coro)
        t._log_destroy_pending = False
        return t


class MockServerContext(LoopContext):
    def given_a_mock_server_on_the_other_end_of_the_transport(self):
        self.dispatcher = protocol.Dispatcher()
        self.protocol = protocol.AMQP(self.dispatcher, self.loop)
        self.server = MockServer(self.protocol)
        self.transport = FakeTransport(self.server)
        self.protocol.connection_made(self.transport)


class OpenConnectionContext(MockServerContext):
    def given_an_open_connection(self):
        connection_info = ConnectionInfo('guest', 'guest', '/')
        task = asyncio.async(open_connection(self.loop, self.protocol, self.dispatcher, connection_info))
        self.tick()

        start_method = spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.server.send_method(0, start_method)
        self.tick()

        tune_method = spec.ConnectionTune(0, 131072, 600)
        self.frame_max = tune_method.frame_max
        self.server.send_method(0, tune_method)
        self.tick()

        self.server.send_method(0, spec.ConnectionOpenOK(''))
        self.tick()

        self.connection = task.result()


class OpenChannelContext(OpenConnectionContext):
    def given_an_open_channel(self):
        self.channel = self.open_channel()

    def open_channel(self, channel_id=1):
        task = asyncio.async(self.connection.open_channel(), loop=self.loop)
        self.tick()
        self.server.send_method(channel_id, spec.ChannelOpenOK(''))
        self.tick()
        return task.result()


class QueueContext(OpenChannelContext):
    def given_a_queue(self):
        queue_name = 'my.nice.queue'
        task = asyncio.async(self.channel.declare_queue(queue_name, durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()
        self.server.send_method(self.channel.id, spec.QueueDeclareOK(queue_name, 123, 456))
        self.tick()
        self.queue = task.result()


class ExchangeContext(OpenChannelContext):
    def given_an_exchange(self):
        self.exchange = self.make_exchange('my.nice.exchange')

    def make_exchange(self, name):
        task = asyncio.async(self.channel.declare_exchange(name, 'fanout', durable=True, auto_delete=False, internal=False),
                             loop=self.loop)
        self.tick()
        self.server.send_method(self.channel.id, spec.ExchangeDeclareOK())
        self.tick()
        return task.result()


class BoundQueueContext(QueueContext, ExchangeContext):
    def given_a_bound_queue(self):
        task = asyncio.async(self.queue.bind(self.exchange, 'routing.key'))
        self.tick()
        self.server.send_method(self.channel.id, spec.QueueBindOK())
        self.tick()
        self.binding = task.result()


class ConsumerContext(QueueContext):
    def given_a_consumer(self):
        self.callback = mock.Mock()
        del self.callback._is_coroutine  # :(

        task = asyncio.async(self.queue.consume(self.callback, no_local=False, no_ack=False, exclusive=False))
        self.tick()
        self.server.send_method(self.channel.id, spec.BasicConsumeOK('made.up.tag'))
        self.tick()
        self.consumer = task.result()


class MockLoopContext(LoopContext):
    def given_an_event_loop(self):
        self.loop = mock.Mock(spec=asyncio.AbstractEventLoop)


class LegacyConnectionContext(LoopContext):
    def given_the_pieces_i_need_for_a_connection(self):
        self.protocol = mock.Mock(spec=protocol.AMQP)
        self.protocol.transport = mock.Mock()
        self.protocol.send_frame._is_coroutine = False  # :(

        self.dispatcher = protocol.Dispatcher()
        self.connection_info = ConnectionInfo('guest', 'guest', '/')


class LegacyOpenConnectionContext(LegacyConnectionContext):
    def given_an_open_connection(self):
        task = asyncio.async(open_connection(self.loop, self.protocol, self.dispatcher, self.connection_info))
        self.tick()

        start_frame = asynqp.frames.MethodFrame(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))
        self.dispatcher.dispatch(start_frame)
        self.tick()

        self.frame_max = 131072
        tune_frame = asynqp.frames.MethodFrame(0, spec.ConnectionTune(0, self.frame_max, 600))
        self.dispatcher.dispatch(tune_frame)
        self.tick()

        open_ok_frame = asynqp.frames.MethodFrame(0, spec.ConnectionOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        self.tick()

        self.protocol.reset_mock()
        self.connection = task.result()


class ProtocolContext(LoopContext):
    def given_a_connected_protocol(self):
        self.transport = mock.Mock(spec=asyncio.Transport)
        self.dispatcher = protocol.Dispatcher()
        self.protocol = protocol.AMQP(self.dispatcher, self.loop)
        self.protocol.connection_made(self.transport)


class MockDispatcherContext(LoopContext):
    def given_a_connected_protocol(self):
        self.transport = mock.Mock(spec=asyncio.Transport)
        self.dispatcher = mock.Mock(spec=protocol.Dispatcher)
        self.protocol = protocol.AMQP(self.dispatcher, self.loop)
        self.protocol.connection_made(self.transport)
