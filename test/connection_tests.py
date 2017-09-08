import asyncio
import sys
from contextlib import suppress
import contexts
from asynqp import spec, exceptions
from asynqp.connection import open_connection
from .base_contexts import MockServerContext, OpenConnectionContext


class WhenRespondingToConnectionStart(MockServerContext):
    def given_I_wrote_the_protocol_header(self):
        connection_info = {'username': 'guest', 'password': 'guest', 'virtual_host': '/'}
        self.async_partial(open_connection(self.loop, self.transport, self.protocol, self.dispatcher, connection_info))

    def when_ConnectionStart_arrives(self):
        self.server.send_method(0, spec.ConnectionStart(0, 9, {}, 'PLAIN AMQPLAIN', 'en_US'))

    def it_should_send_start_ok(self):
        expected_method = spec.ConnectionStartOK(
            {"product": "asynqp", "version": "0.1", "platform": sys.version,
             "capabilities": {
                 "consumer_cancel_notify": True
             }},
            'AMQPLAIN',
            {'LOGIN': 'guest', 'PASSWORD': 'guest'},
            'en_US'
        )
        self.server.should_have_received_method(0, expected_method)


class WhenRespondingToConnectionTune(MockServerContext):
    def given_a_started_connection(self):
        connection_info = {'username': 'guest', 'password': 'guest', 'virtual_host': '/'}
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

    def it_should_not_block_clonnection_close(self):
        self.loop.run_until_complete(
            asyncio.wait_for(self.connection.close(), 0.2))


class WhenTheApplicationClosesTheConnection(OpenConnectionContext):
    def when_I_close_the_connection(self):
        self.async_partial(self.connection.close())

    def it_should_send_ConnectionClose_with_no_exception(self):
        expected = spec.ConnectionClose(0, 'Connection closed by application', 0, 0)
        self.server.should_have_received_method(0, expected)


class WhenRecievingConnectionCloseOK(OpenConnectionContext):
    def given_a_connection_that_I_closed(self):
        asyncio.ensure_future(self.connection.close())
        self.tick()

    def when_connection_close_ok_arrives(self):
        self.server.send_method(0, spec.ConnectionCloseOK())
        self.tick()

    def it_should_close_the_transport(self):
        assert self.transport.closed


class WhenAConnectionThatIsClosingReceivesAMethod(OpenConnectionContext):
    def given_a_closed_connection(self):
        t = asyncio.ensure_future(self.connection.close())
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


class WhenAConnectionIsLostCloseConnection(OpenConnectionContext):
    def when_connection_is_closed(self):
        with suppress(Exception):
            self.connection.protocol.connection_lost(Exception())

    def it_should_not_hang(self):
        self.loop.run_until_complete(asyncio.wait_for(self.connection.close(), 0.2))


class WhenServerClosesTransportWithoutConnectionClose(OpenConnectionContext):

    def given_a_channel(self):
        task = self.loop.create_task(self.connection.open_channel())
        self.tick()
        self.server.send_method(1, spec.ChannelOpenOK(''))
        self.channel = self.wait_for(task)

    def when_server_closes_transport(self):
        with suppress(exceptions.ConnectionLostError):
            self.protocol.connection_lost(None)

    def it_should_raise_error_in_connection_methods(self):
        try:
            self.wait_for(self.channel.declare_queue("some.queue"))
        except exceptions.ConnectionLostError as err:
            assert type(err) == exceptions.ConnectionLostError
        else:
            assert False, "ConnectionLostError not raised"


class WhenOpeningAChannelOnAClosedConnection(OpenConnectionContext):
    def when_client_closes_connection(self):
        task = asyncio.ensure_future(self.connection.close())
        self.tick()
        self.server.send_method(0, spec.ConnectionCloseOK())
        self.tick()
        self.tick()
        task.result()

    def it_should_raise_error_in_connection_methods(self):
        exc = contexts.catch(self.wait_for, self.connection.open_channel())
        assert isinstance(exc, exceptions.ConnectionClosed)


class WhenServerAndClientCloseConnectionAtATime(OpenConnectionContext):
    def when_both_sides_close_channel(self):
        # Client tries to close connection
        self.task = asyncio.ensure_future(self.connection.close(), loop=self.loop)
        self.tick()
        # Before OK arrives server closes connection
        self.server.send_method(
            0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.tick()
        self.tick()
        self.task.result()

    def if_should_have_closed_connection(self):
        assert self.connection._closing

    def it_should_have_killed_synchroniser_with_server_error(self):
        assert isinstance(
            self.connection.synchroniser.connection_exc,
            exceptions.ConnectionClosed)
        assert self.connection.synchroniser.connection_exc.reply_code == 123
