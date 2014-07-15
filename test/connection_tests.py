import asyncio
import sys
import asynqp
from unittest import mock
from asynqp import spec
from asynqp.connection import open_connection
from .base_contexts import ConnectionContext, OpenConnectionContext


class WhenRespondingToConnectionStart(ConnectionContext):
    def given_I_wrote_the_protocol_header(self):
        asyncio.async(open_connection(self.loop, self.protocol, self.dispatcher, self.connection_info))
        self.tick()

    def when_ConnectionStart_arrives(self):
        start_method = spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.dispatcher.dispatch(asynqp.frames.MethodFrame(0, start_method))
        self.tick()

    def it_should_send_start_ok(self):
        expected_method = spec.ConnectionStartOK(
            {"product": "asynqp", "version": mock.ANY, "platform": sys.version},
            'AMQPLAIN',
            {'LOGIN': 'guest', 'PASSWORD': 'guest'},
            'en_US'
        )
        self.protocol.send_method.assert_called_once_with(0, expected_method)


class WhenRespondingToConnectionTune(ConnectionContext):
    def given_a_started_connection(self):
        asyncio.async(open_connection(self.loop, self.protocol, self.dispatcher, self.connection_info))
        self.tick()
        start_method = spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.dispatcher.dispatch(asynqp.frames.MethodFrame(0, start_method))
        self.tick()

    def when_ConnectionTune_arrives(self):
        tune_frame = asynqp.frames.MethodFrame(0, spec.ConnectionTune(0, 131072, 600))
        self.dispatcher.dispatch(tune_frame)

    def it_should_send_tune_ok_followed_by_open(self):
        tune_ok = spec.ConnectionTuneOK(0, 131072, 600)
        open_method = spec.ConnectionOpen('/', '', False)
        self.protocol.send_method.assert_has_calls([mock.call(0, tune_ok), mock.call(0, open_method)])

    def it_should_start_heartbeating(self):
        self.protocol.start_heartbeat.assert_called_once_with(600)


class WhenRespondingToConnectionClose(OpenConnectionContext):
    def when_the_close_frame_arrives(self):
        close_frame = asynqp.frames.MethodFrame(0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.dispatcher.dispatch(close_frame)

    def it_should_send_close_ok(self):
        expected = spec.ConnectionCloseOK()
        self.protocol.send_method.assert_called_once_with(0, expected)


class WhenAConnectionThatWasClosedByTheServerReceivesAMethod(OpenConnectionContext):
    def given_a_closed_connection(self):
        close_frame = asynqp.frames.MethodFrame(0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.dispatcher.dispatch(close_frame)
        self.tick()
        self.mock_handler = mock.Mock()

    def when_another_frame_arrives(self):
        unexpected_frame = asynqp.frames.MethodFrame(1, spec.BasicDeliver('', 1, False, '', ''))

        with mock.patch.dict(self.dispatcher.handlers, {1: self.mock_handler}):
            self.dispatcher.dispatch(unexpected_frame)

    def it_MUST_be_discarded(self):
        assert not self.mock_handler.method_calls


class WhenAConnectionThatWasClosedByTheApplicationReceivesAMethod(OpenConnectionContext):
    def given_a_closed_connection(self):
        asyncio.async(self.connection.close())
        self.tick()

        start_method = spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.frames.MethodFrame(0, start_method)
        self.mock_handler = mock.Mock()

    def when_another_frame_arrives(self):
        with mock.patch.dict(self.dispatcher.handlers, {0: self.mock_handler}):
            self.dispatcher.dispatch(self.start_frame)

    def it_MUST_be_discarded(self):
        assert not self.mock_handler.method_calls


class WhenTheApplicationClosesTheConnection(OpenConnectionContext):
    def when_I_close_the_connection(self):
        asyncio.async(self.connection.close())
        self.tick()

    def it_should_send_ConnectionClose_with_no_exception(self):
        expected = spec.ConnectionClose(0, 'Connection closed by application', 0, 0)
        self.protocol.send_method.assert_called_once_with(0, expected)


class WhenRecievingConnectionCloseOK(OpenConnectionContext):
    def given_a_connection_that_I_closed(self):
        asyncio.async(self.connection.close())
        self.tick()

    def when_connection_close_ok_arrives(self):
        frame = asynqp.frames.MethodFrame(0, spec.ConnectionCloseOK())
        self.dispatcher.dispatch(frame)

    def it_should_close_the_transport(self):
        assert self.protocol.transport.close.called
