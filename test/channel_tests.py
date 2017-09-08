import asyncio
import contexts
import asynqp
from contextlib import suppress
from unittest import mock
from asynqp import spec, frames, exceptions
from asynqp import message
from . import util
from .base_contexts import OpenConnectionContext, OpenChannelContext


class WhenOpeningAChannel(OpenConnectionContext):
    def when_the_user_wants_to_open_a_channel(self):
        self.async_partial(self.connection.open_channel())

    def it_should_send_a_channel_open_frame(self):
        self.server.should_have_received_method(1, spec.ChannelOpen(''))


class WhenOpeningMultipleChannelsConcurrently(OpenConnectionContext):
    def when_the_user_wants_to_open_several_channels(self):
        self.async_partial(asyncio.wait([self.connection.open_channel(), self.connection.open_channel()]))

    def it_should_send_a_channel_open_frame_for_channel_1(self):
        self.server.should_have_received_method(1, spec.ChannelOpen(''))

    def it_should_send_a_channel_open_frame_for_channel_2(self):
        self.server.should_have_received_method(2, spec.ChannelOpen(''))


class WhenChannelOpenOKArrives(OpenConnectionContext):
    def given_the_user_has_called_open_channel(self):
        self.task = asyncio.ensure_future(self.connection.open_channel())
        self.tick()

    def when_channel_open_ok_arrives(self):
        self.server.send_method(1, spec.ChannelOpenOK(''))

    def it_should_have_the_correct_channel_id(self):
        assert self.task.result().id == 1


class WhenOpeningASecondChannel(OpenChannelContext):
    def when_the_user_opens_another_channel(self):
        self.result = self.open_channel(2)

    def it_should_send_another_channel_open_frame(self):
        self.server.should_have_received_method(2, spec.ChannelOpen(''))

    def it_should_have_the_correct_channel_id(self):
        assert self.result.id == 2


class WhenTheApplicationClosesAChannel(OpenChannelContext):
    def when_I_close_the_channel(self):
        self.async_partial(self.channel.close())

    def it_should_send_ChannelClose(self):
        self.server.should_have_received_method(1, spec.ChannelClose(0, 'Channel closed by application', 0, 0))


class WhenTheServerClosesAChannel(OpenChannelContext):
    def when_the_server_shuts_the_channel_down(self):
        self.server.send_method(self.channel.id, spec.ChannelClose(404, 'i am tired of you', 40, 50))

    def it_should_send_ChannelCloseOK(self):
        self.server.should_have_received_method(self.channel.id, spec.ChannelCloseOK())


class WhenAnotherMethodArrivesWhileTheChannelIsClosing(OpenChannelContext):
    def given_that_i_closed_the_channel(self):
        self.async_partial(self.channel.close())
        self.server.reset()

    def when_another_method_arrives(self):
        self.server.send_method(self.channel.id, spec.ChannelOpenOK(''))

    def it_MUST_discard_the_method(self):
        self.server.should_not_have_received_any()


class WhenAnotherMethodArrivesAfterTheServerClosedTheChannel(OpenChannelContext):
    def given_the_server_closed_the_channel(self):
        self.server.send_method(self.channel.id, spec.ChannelClose(404, 'i am tired of you', 40, 50))
        self.server.reset()

    def when_another_method_arrives(self):
        self.server.send_method(self.channel.id, spec.ChannelOpenOK(''))

    def it_MUST_discard_the_method(self):
        self.server.should_not_have_received_any()


class WhenAnAsyncMethodArrivesWhileWeAwaitASynchronousOne(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        self.task = self.async_partial(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True))
        self.server.reset()

    def when_an_async_method_arrives(self):
        with util.silence_expected_destroy_pending_log('receive_deliver'):
            self.server.send_method(self.channel.id, spec.BasicDeliver('consumer', 2, False, 'exchange', 'routing_key'))

    def it_should_not_close_the_channel(self):
        self.server.should_not_have_received_any()

    def it_should_not_throw_an_exception(self):
        assert not self.task.done()


class WhenAnUnexpectedChannelCloseArrives(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        self.task = asyncio.ensure_future(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True))
        self.tick()

    def when_ChannelClose_arrives(self):
        self.server.send_method(self.channel.id, spec.ChannelClose(406, "the precondition, she failed", 50, 10))
        self.tick()

    def it_should_send_ChannelCloseOK(self):
        self.server.should_have_received_method(self.channel.id, spec.ChannelCloseOK())

    def it_should_throw_an_exception(self):
        assert isinstance(self.task.exception(), exceptions.PreconditionFailed)


class WhenSettingQOS(OpenChannelContext):
    def when_we_are_setting_prefetch_count_only(self):
        self.async_partial(self.channel.set_qos(prefetch_size=1000, prefetch_count=100, apply_globally=True))

    def it_should_send_BasicQos_with_default_values(self):
        self.server.should_have_received_method(self.channel.id, spec.BasicQos(1000, 100, True))


class WhenBasicQOSOkArrives(OpenChannelContext):
    def given_we_are_setting_qos_settings(self):
        self.task = asyncio.ensure_future(self.channel.set_qos(prefetch_size=1000, prefetch_count=100, apply_globally=True))
        self.tick()

    def when_BasicQosOk_arrives(self):
        self.server.send_method(self.channel.id, spec.BasicQosOK())

    def it_should_yield_result(self):
        assert self.task.done()


class WhenBasicReturnArrivesAndIHaveDefinedAHandler(OpenChannelContext):
    def given_a_message(self):
        self.expected_message = asynqp.Message('body')

        self.callback = mock.Mock()
        del self.callback._is_coroutine  # :(
        self.channel.set_return_handler(self.callback)

    def when_BasicReturn_arrives_with_content(self):
        method = spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key")
        self.server.send_frame(frames.MethodFrame(self.channel.id, method))

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

    def it_should_send_the_message_to_the_callback(self):
        self.callback.assert_called_once_with(self.expected_message)


class WhenBasicReturnArrivesAndIHaveNotDefinedAHandler(OpenChannelContext):
    def given_I_am_listening_for_asyncio_exceptions(self):
        self.expected_message = asynqp.Message('body')

        self.exception = None
        self.loop.set_exception_handler(lambda l, c: setattr(self, "exception", c["exception"]))

    def when_BasicReturn_arrives(self):
        method = spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key")
        self.server.send_frame(frames.MethodFrame(self.channel.id, method))

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()

    def it_should_throw_an_exception(self):
        assert self.exception is not None

    def it_should_set_the_reply_code(self):
        assert self.exception.args == (self.expected_message,)

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(util.testing_exception_handler)


# test that the call to handler.ready() happens at the correct time
class WhenBasicReturnArrivesAfterThrowingTheExceptionOnce(OpenChannelContext):
    def given_I_am_listening_for_asyncio_exceptions(self):
        self.expected_message = asynqp.Message('body')

        self.exception = None
        self.loop.set_exception_handler(lambda l, c: setattr(self, "exception", c["exception"]))

        self.return_msg()  # cause basic_return exception to be thrown
        self.exception = None  # reset self.exception

    def when_BasicReturn_arrives(self):
        self.return_msg()

    def it_should_throw_the_exception_again(self):
        assert self.exception is not None

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(util.testing_exception_handler)

    def return_msg(self):
        method = spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key")
        self.server.send_frame(frames.MethodFrame(self.channel.id, method))

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.server.send_frame(frames.ContentHeaderFrame(self.channel.id, header))

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.server.send_frame(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()


class WhenTheHandlerIsNotCallable(OpenChannelContext):
    def when_I_set_the_handler(self):
        self.exception = contexts.catch(self.channel.set_return_handler, "i am not callable")

    def it_should_throw_a_TypeError(self):
        assert isinstance(self.exception, TypeError)


class WhenAConnectionIsLostCloseChannel(OpenChannelContext):
    def when_connection_is_closed(self):
        with suppress(Exception):
            self.connection.protocol.connection_lost(Exception())
        self.tick()
        self.was_closed = self.channel.is_closed()

    def it_should_not_hang(self):
        self.loop.run_until_complete(asyncio.wait_for(self.channel.close(), 0.2))

    def if_should_have_closed_channel(self):
        assert self.was_closed


class WhenWeCloseConnectionChannelShouldAlsoClose(OpenChannelContext):
    def when_connection_is_closed(self):
        self.task = asyncio.ensure_future(self.connection.close(), loop=self.loop)
        self.server.send_method(0, spec.ConnectionCloseOK())
        self.tick()
        self.was_closed = self.channel.is_closed()
        self.loop.run_until_complete(asyncio.wait_for(self.task, 0.2))

    def it_should_not_hang_channel_close(self):
        self.loop.run_until_complete(asyncio.wait_for(self.channel.close(), 0.2))

    def if_should_have_closed_channel(self):
        assert self.was_closed


class WhenServerClosesConnectionChannelShouldAlsoClose(OpenChannelContext):
    def when_connection_is_closed(self):
        self.server.send_method(
            0, spec.ConnectionClose(123, 'you muffed up', 10, 20))
        self.tick()
        self.was_closed = self.channel.is_closed()

    def it_should_not_hang_channel_close(self):
        self.loop.run_until_complete(asyncio.wait_for(self.channel.close(), 0.2))

    def if_should_have_closed_channel(self):
        assert self.was_closed


class WhenServerAndClientCloseChannelAtATime(OpenChannelContext):
    def when_both_sides_close_channel(self):
        # Client tries to close connection
        self.task = asyncio.ensure_future(self.channel.close(), loop=self.loop)
        self.tick()
        # Before OK arrives server closes connection
        self.server.send_method(
            self.channel.id,
            spec.ChannelClose(404, 'i am tired of you', 40, 50))
        self.tick()
        self.task.result()

    def if_should_have_closed_channel(self):
        assert self.channel.is_closed()

    def it_should_have_killed_synchroniser_with_404(self):
        assert self.channel.synchroniser.connection_exc == exceptions.NotFound
