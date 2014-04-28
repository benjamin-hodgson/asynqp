import asynqp
from asynqp import methods
from unittest import mock


class ConnectionContext:
    def given_a_connection(self):
        self.connection = asynqp.Connection('guest', 'guest')
        self.connection.protocol = self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.protocol.transport = mock.Mock()


class WhenRespondingToConnectionStart(ConnectionContext):
    def given_a_start_frame_from_the_server(self):
        start_method = methods.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.Frame(asynqp.FrameType.method, 0, start_method)

    def because_the_start_frame_arrives(self):
        self.connection.dispatch(self.start_frame)  # TODO: what does the server do when the credentials are wrong?

    def it_should_send_start_ok(self):
        expected_method = methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN': 'guest', 'PASSWORD': 'guest'}, 'en_US')
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, expected_method)
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenRespondingToConnectionTune(ConnectionContext):
    def given_a_tune_frame_from_the_server(self):
        self.tune_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTune(0, 131072, 600))

    def when_the_tune_frame_arrives(self):
        self.connection.dispatch(self.tune_frame)

    def it_should_send_tune_ok_followed_by_open(self):
        tune_ok_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionTuneOK(1024, 0, 0))
        open_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionOpen('/', '', False))
        self.protocol.send_frame.assert_has_calls([mock.call(tune_ok_frame), mock.call(open_frame)])


class WhenRespondingToConnectionClose(ConnectionContext):
    def given_a_close_frame_from_the_server(self):
        self.close_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionClose(123, 'you muffed up', 10, 20))

    def when_the_close_frame_arrives(self):
        self.connection.dispatch(self.close_frame)

    def it_should_send_close_ok(self):
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionCloseOK())
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenAConnectionThatWasClosedByTheServerRecievesAMethod(ConnectionContext):
    def given_a_closed_connection(self):
        close_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionClose(123, 'you muffed up', 10, 20))
        self.connection.dispatch(close_frame)

        start_method = methods.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US')
        self.start_frame = asynqp.Frame(asynqp.FrameType.method, 0, start_method)
        self.mock_handler = mock.Mock()

    def when_another_frame_arrives(self):
        with mock.patch.object(self.connection, 'handle_ConnectionStart', self.mock_handler):
            self.connection.dispatch(self.start_frame)

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
            self.connection.dispatch(self.start_frame)

    def it_should_not_dispatch_the_frame(self):
        assert not self.mock_handler.called


class WhenTheApplicationClosesTheConnection(ConnectionContext):
    def when_i_close_the_connection(self):
        self.connection.close()

    def it_should_send_ConnectionClose_with_no_exception(self):
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionClose(0, '', 0, 0))
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenRecievingConnectionCloseOK(ConnectionContext):
    def given_a_connection_that_I_closed(self):
        self.connection.close()

    def when_connection_close_ok_arrives(self):
        frame = asynqp.Frame(asynqp.FrameType.method, 0, methods.ConnectionCloseOK())
        self.connection.dispatch(frame)

    def it_should_close_the_transport(self):
        assert self.protocol.transport.close.called
