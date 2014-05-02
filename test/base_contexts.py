import asyncio
import asynqp
from asyncio import test_utils
from asynqp import spec
from unittest import mock


class LoopContext:
    def given_an_event_loop(self):
        self.loop = asyncio.get_event_loop()


class MockLoopContext(LoopContext):
    def given_an_event_loop(self):
        self.loop = mock.Mock(spec=asyncio.AbstractEventLoop)


class ConnectionContext(LoopContext):
    def given_a_connection(self):
        self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.protocol.send_frame._is_coroutine = False  # :(
        self.dispatcher = asynqp.Dispatcher(self.loop)
        self.connection_info = asynqp.ConnectionInfo('guest', 'guest', '/')
        self.connection = asynqp.Connection(self.loop, self.protocol, self.dispatcher, self.connection_info)
        self.protocol.transport = mock.Mock()


class OpenConnectionContext(ConnectionContext):
    def given_an_open_connection(self):
        start_frame = asynqp.frames.MethodFrame(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))
        self.dispatcher.dispatch(start_frame)

        tune_frame = asynqp.frames.MethodFrame(0, spec.ConnectionTune(0, 131072, 600))
        self.dispatcher.dispatch(tune_frame)

        open_ok_frame = asynqp.frames.MethodFrame(0, spec.ConnectionOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        self.protocol.reset_mock()


class ProtocolContext(LoopContext):
    def given_a_connected_protocol(self):
        self.transport = mock.Mock(spec=asyncio.Transport)
        self.dispatcher = asynqp.Dispatcher(self.loop)
        self.protocol = asynqp.AMQP(self.dispatcher, self.loop)
        self.protocol.connection_made(self.transport)


class OpenChannelContext(OpenConnectionContext):
    def given_an_open_channel(self):
        self.channel = self.open_channel()
        self.protocol.reset_mock()

    def open_channel(self, channel_id=1):
        task = asyncio.async(self.connection.open_channel(), loop=self.loop)
        test_utils.run_briefly(self.loop)
        open_ok_frame = asynqp.frames.MethodFrame(channel_id, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        test_utils.run_briefly(self.loop)
        return task.result()
