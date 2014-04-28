import asyncio
import asynqp
from asynqp import methods
from unittest import mock


class ConnectionContext:
    def given_a_connection(self):
        self.loop = mock.Mock(spec=asyncio.AbstractEventLoop)
        self.connection = asynqp.Connection('guest', 'guest', loop=self.loop)
        self.connection.protocol = self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.protocol.transport = mock.Mock()


class WhenRespondingToConnectionStart(ConnectionContext):
    def given_a_start_frame_from_the_server(self):
        start_method = methods.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.Frame(asynqp.FrameType.method, 0, start_method)

    def because_the_start_frame_arrives(self):
        self.connection.handle(self.start_frame)  # TODO: what does the server do when the credentials are wrong?

    def it_should_send_start_ok(self):
        expected_method = methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN': 'guest', 'PASSWORD': 'guest'}, 'en_US')
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, expected_method)
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenRespondingToConnectionTune(ConnectionContext):
    def given_a_tune_frame_from_the_server(self):
        self.tune_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTune(0, 131072, 600))

    def when_the_tune_frame_arrives(self):
        self.connection.handle(self.tune_frame)

    def it_should_send_tune_ok_followed_by_open(self):
        tune_ok_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTuneOK(1024, 131072, 600))
        open_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionOpen('/', '', False))
        self.protocol.send_frame.assert_has_calls([mock.call(tune_ok_frame), mock.call(open_frame)])

    def it_should_start_sending_and_monitoring_heartbeats(self):
        self.loop.call_later.assert_has_calls([
            mock.call(600, self.connection.send_heartbeat),
            mock.call(600*2, self.connection.close, 501, 'Heartbeat timed out')
        ])


class WhenItIsTimeToHeartbeat(ConnectionContext):
    def given_an_open_connection(self):
        self.tune_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTune(0, 131072, 600))
        self.connection.handle(self.tune_frame)
        self.protocol.reset_mock()

    def when_i_send_the_heartbeat(self):
        self.connection.send_heartbeat()

    def it_should_send_a_heartbeat_frame(self):
        self.protocol.send_frame.assert_called_once_with(asynqp.Frame(asynqp.FrameType.heartbeat, 0, b''))

    def it_should_set_up_the_next_heartbeat(self):
        assert len(self.loop.call_later.call_args_list) == 3
        self.loop.call_later.assert_called_with(600, self.connection.send_heartbeat)


class WhenResettingTheHeartbeatTimeout(ConnectionContext):
    def given_an_open_connection(self):
        self.tune_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTune(0, 131072, 600))
        self.connection.handle(self.tune_frame)

    def because_the_timeout_gets_reset(self):
        self.connection.reset_heartbeat_timeout()

    def it_should_cancel_the_close_callback(self):
        self.loop.call_later.return_value.cancel.assert_called_once_with()

    def it_should_set_up_another_close_callback(self):
        assert len(self.loop.call_later.call_args_list) == 3
        self.loop.call_later.assert_called_with(600*2, self.connection.close, 501, 'Heartbeat timed out')


class WhenRespondingToConnectionClose(ConnectionContext):
    def given_a_close_frame_from_the_server(self):
        self.close_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionClose(123, 'you muffed up', 10, 20))

    def when_the_close_frame_arrives(self):
        self.connection.handle(self.close_frame)

    def it_should_send_close_ok(self):
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionCloseOK())
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenAConnectionThatWasClosedByTheServerRecievesAMethod(ConnectionContext):
    def given_a_closed_connection(self):
        close_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionClose(123, 'you muffed up', 10, 20))
        self.connection.handle(close_frame)

        start_method = methods.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.Frame(asynqp.FrameType.method, 0, start_method)
        self.mock_handler = mock.Mock()

    def when_another_frame_arrives(self):
        with mock.patch.object(self.connection, 'handle_ConnectionStart', self.mock_handler):
            self.connection.handle(self.start_frame)

    def it_MUST_be_discarded(self):
        assert not self.mock_handler.called


class WhenAConnectionThatWasClosedByTheApplicationRecievesAMethod(ConnectionContext):
    def given_a_closed_connection(self):
        self.connection.close()

        start_method = methods.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.Frame(asynqp.FrameType.method, 0, start_method)
        self.mock_handler = mock.Mock()

    def when_another_frame_arrives(self):
        with mock.patch.object(self.connection, 'handle_ConnectionStart', self.mock_handler):
            self.connection.handle(self.start_frame)

    def it_should_not_dispatch_the_frame(self):
        assert not self.mock_handler.called


class WhenTheApplicationClosesTheConnection(ConnectionContext):
    def when_i_close_the_connection(self):
        self.connection.close()

    def it_should_send_ConnectionClose_with_no_exception(self):
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionClose(0, 'Connection closed by application', 0, 0))
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenRecievingConnectionCloseOK(ConnectionContext):
    def given_a_connection_that_I_closed(self):
        self.connection.close()

    def when_connection_close_ok_arrives(self):
        frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionCloseOK())
        self.connection.handle(frame)

    def it_should_close_the_transport(self):
        assert self.protocol.transport.close.called
