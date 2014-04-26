import asynqp
from unittest import mock


class ConnectionContext:
    def given_a_connection(self):
        self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.connection = asynqp.Connection(self.protocol, 'guest', 'guest')


class WhenRespondingToConnectionStart(ConnectionContext):
    def given_a_start_frame_from_the_server(self):
        start_method = asynqp.methods.ConnectionStart(0, 9, {}, ['PLAIN', 'AMQPLAIN'], ['en_US'])
        self.start_frame = asynqp.Frame(asynqp.FrameType.method, 0, start_method)
        expected_method = asynqp.methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN':'guest', 'PASSWORD':'guest'}, 'en_US')
        self.expected_frame = asynqp.Frame(asynqp.FrameType.method, 0, expected_method)

    def because_the_start_frame_arrives(self):
        self.connection.handle_ConnectionStart(self.start_frame)  # TODO: what does the server do when the credentials are wrong?

    def it_should_send_start_ok(self):
        self.protocol.send_frame.assert_called_once_with(self.expected_frame)


class WhenRespondingToConnectionTune(ConnectionContext):
    def given_a_tune_frame_from_the_server(self):
        self.tune_frame = asynqp.Frame(asynqp.FrameType.method, 0, asynqp.methods.ConnectionTune(0, 131072, 600))
        self.tune_ok_frame = asynqp.Frame(asynqp.FrameType.method, 0, asynqp.methods.ConnectionTuneOK(1024, 0, 0))
        self.open_frame = asynqp.Frame(asynqp.FrameType.method, 0, asynqp.methods.ConnectionOpen('/'))

    def when_the_tune_frame_arrives(self):
        self.connection.handle_ConnectionTune(self.tune_frame)

    def it_should_send_tune_ok_followed_by_open(self):
        self.protocol.send_frame.assert_has_calls([mock.call(self.tune_ok_frame), mock.call(self.open_frame)])
