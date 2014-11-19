import asyncio
import sys
import asynqp
from unittest import mock
from asynqp import spec, protocol, frames
from asynqp.connection import open_connection, ConnectionInfo
from .base_contexts import LegacyOpenConnectionContext, MockServerContext, OpenConnectionContext


class WhenRespondingToConnectionStart(MockServerContext):
    def given_I_wrote_the_protocol_header(self):
        connection_info = ConnectionInfo('guest', 'guest', '/')
        self.async_partial(open_connection(self.loop, self.protocol, self.dispatcher, connection_info))
        self.tick()

    def when_ConnectionStart_arrives(self):
        self.server.send_method(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))
        self.tick()

    def it_should_send_start_ok(self):
        expected_method = spec.ConnectionStartOK(
            {"product": "asynqp", "version": "0.1", "platform": sys.version},
            'AMQPLAIN',
            {'LOGIN': 'guest', 'PASSWORD': 'guest'},
            'en_US'
        )
        self.server.should_have_received_method(0, expected_method)


class WhenRespondingToConnectionTune(MockServerContext):
    def given_a_started_connection(self):
        connection_info = ConnectionInfo('guest', 'guest', '/')
        self.async_partial(open_connection(self.loop, self.protocol, self.dispatcher, connection_info))
        self.tick()
        self.server.send_method(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))
        self.tick()

    def when_ConnectionTune_arrives(self):
        self.server.send_method(0, spec.ConnectionTune(0, 131072, 600))
        self.tick()

    def it_should_send_tune_ok_followed_by_open(self):
        tune_ok_method = spec.ConnectionTuneOK(0, 131072, 600)
        open_method = spec.ConnectionOpen('/', '', False)
        self.server.should_have_received_methods(0, [tune_ok_method, open_method])


class WhenRespondingToConnectionClose(OpenConnectionContext):
    def when_the_close_frame_arrives(self):
        self.server.send_method(0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.tick()

    def it_should_send_close_ok(self):
        self.server.should_have_received_method(0, spec.ConnectionCloseOK())


class WhenTheApplicationClosesTheConnection(OpenConnectionContext):
    def when_I_close_the_connection(self):
        self.async_partial(self.connection.close())
        self.tick()

    def it_should_send_ConnectionClose_with_no_exception(self):
        expected = spec.ConnectionClose(0, 'Connection closed by application', 0, 0)
        self.server.should_have_received_method(0, expected)


class WhenRecievingConnectionCloseOK(OpenConnectionContext):
    def given_a_connection_that_I_closed(self):
        asyncio.async(self.connection.close())
        self.tick()

    def when_connection_close_ok_arrives(self):
        self.server.send_method(0, spec.ConnectionCloseOK())
        self.tick()

    def it_should_close_the_transport(self):
        assert self.transport.closed


# TODO: rewrite me to use a handler, not a queue writer
class WhenAConnectionThatIsClosingReceivesAMethod(LegacyOpenConnectionContext):
    def given_a_closed_connection(self):
        t = asyncio.async(self.connection.close())
        t._log_destroy_pending = False
        self.tick()

        start_method = spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.frames.MethodFrame(0, start_method)
        self.mock_writer = mock.Mock()

    def when_another_frame_arrives(self):
        with mock.patch.dict(self.dispatcher.queue_writers, {0: self.mock_writer}):
            self.dispatcher.dispatch(self.start_frame)
            self.tick()

    def it_MUST_be_discarded(self):
        assert not self.mock_writer.method_calls


# TODO: rewrite so it doesn't know about dispatcher
class WhenAConnectionThatWasClosedByTheServerReceivesAMethod(LegacyOpenConnectionContext):
    def given_a_closed_connection(self):
        close_frame = asynqp.frames.MethodFrame(0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.dispatcher.dispatch(close_frame)
        self.tick()
        self.mock_writer = mock.Mock()

    def when_another_frame_arrives(self):
        unexpected_frame = asynqp.frames.MethodFrame(1, spec.BasicDeliver('', 1, False, '', ''))

        with mock.patch.dict(self.dispatcher.queue_writers, {1: self.mock_writer}):
            self.dispatcher.dispatch(unexpected_frame)
            self.tick()

    def it_MUST_be_discarded(self):
        assert not self.mock_writer.method_calls
