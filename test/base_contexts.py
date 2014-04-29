import asyncio
import asynqp
from asynqp import methods
from unittest import mock


class LoopContext:
    def given_an_event_loop(self):
        self.loop = asyncio.get_event_loop()


class MockLoopContext(LoopContext):
    def given_an_event_loop(self):
        self.loop = mock.Mock(spec=asyncio.AbstractEventLoop)


class ConnectionContext(LoopContext):
    def given_a_connection(self):
        self.connection = asynqp.Connection('guest', 'guest', loop=self.loop)
        self.connection.protocol = self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.protocol.transport = mock.Mock()


class OpenConnectionContext(ConnectionContext):
    def given_an_open_connection(self):
        start_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))
        self.connection.dispatch(start_frame)

        tune_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTune(0, 131072, 600))
        self.connection.dispatch(tune_frame)

        open_ok_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionOpenOK(''))
        self.connection.dispatch(open_ok_frame)
        self.protocol.reset_mock()


class ProtocolContext:
    def establish_the_connection(self):
        self.transport = mock.Mock(spec=asyncio.Transport)
        self.connection = mock.Mock(spec=asynqp.Connection)
        self.protocol = asynqp.AMQP(self.connection)
        self.connection.protocol = self.protocol
        self.protocol.connection_made(self.transport)
