import asyncio
import asynqp
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
        self.dispatcher = asynqp.Dispatcher()
        self.connection = asynqp.Connection(self.protocol, self.dispatcher, 'guest', 'guest', loop=self.loop)
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


class ProtocolContext:
    def establish_the_connection(self):
        self.transport = mock.Mock(spec=asyncio.Transport)
        self.connection = mock.Mock(spec=asynqp.Connection)
        self.dispatcher = asynqp.Dispatcher()
        self.protocol = asynqp.AMQP(self.dispatcher)
        self.protocol.connection = self.connection
        self.protocol.connection_made(self.transport)
