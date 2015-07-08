import asyncio
import sys
from asynqp import spec
from asynqp.connection import open_connection, ConnectionInfo
from .base_contexts import MockServerContext, OpenConnectionContext


class WhenRespondingToConnectionStart(MockServerContext):
    def given_I_wrote_the_protocol_header(self):
        connection_info = ConnectionInfo('guest', 'guest', '/')
        self.async_partial(open_connection(self.loop, self.transport, self.protocol, self.dispatcher, connection_info))

    def when_ConnectionStart_arrives(self):
        self.server.send_method(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))

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
        self.async_partial(open_connection(self.loop, self.transport, self.protocol, self.dispatcher, connection_info))
        self.server.send_method(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))

    def when_ConnectionTune_arrives(self):
        self.server.send_method(0, spec.ConnectionTune(0, 131072, 600))

    def it_should_send_tune_ok_followed_by_open(self):
        tune_ok_method = spec.ConnectionTuneOK(0, 131072, 600)
        open_method = spec.ConnectionOpen('/', '', False)
        self.server.should_have_received_methods(0, [tune_ok_method, open_method])


class WhenRespondingToConnectionClose(OpenConnectionContext):
    def when_the_close_frame_arrives(self):
        self.server.send_method(0, spec.ConnectionClose(123, 'you muffed up', 10, 20))

    def it_should_send_close_ok(self):
        self.server.should_have_received_method(0, spec.ConnectionCloseOK())


class WhenTheApplicationClosesTheConnection(OpenConnectionContext):
    def when_I_close_the_connection(self):
        self.async_partial(self.connection.close())

    def it_should_send_ConnectionClose_with_no_exception(self):
        expected = spec.ConnectionClose(0, 'Connection closed by application', 0, 0)
        self.server.should_have_received_method(0, expected)


class WhenRecievingConnectionCloseOK(OpenConnectionContext):
    def given_a_connection_that_I_closed(self):
        asyncio.async(self.connection.close())
        self.tick()

    def when_connection_close_ok_arrives(self):
        self.server.send_method(0, spec.ConnectionCloseOK())

    def it_should_close_the_transport(self):
        assert self.transport.closed


class WhenAConnectionThatIsClosingReceivesAMethod(OpenConnectionContext):
    def given_a_closed_connection(self):
        t = asyncio.async(self.connection.close())
        t._log_destroy_pending = False
        self.tick()
        self.server.reset()

    def when_another_frame_arrives(self):
        self.server.send_method(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))
        self.tick()

    def it_MUST_be_discarded(self):
        self.server.should_not_have_received_any()


class WhenAConnectionThatWasClosedByTheServerReceivesAMethod(OpenConnectionContext):
    def given_a_closed_connection(self):
        self.server.send_method(0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.tick()
        self.server.reset()

    def when_another_frame_arrives(self):
        self.server.send_method(0, spec.BasicDeliver('', 1, False, '', ''))
        self.tick()

    def it_MUST_be_discarded(self):
        self.server.should_not_have_received_any()
