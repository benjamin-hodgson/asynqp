import asyncio
import contexts
import asynqp
from unittest import mock
from asynqp import spec, frames
from asynqp import message
from . import util
from .base_contexts import OpenConnectionContext, OpenChannelContext


class WhenOpeningAChannel(OpenConnectionContext):
    def when_the_user_wants_to_open_a_channel(self):
        asyncio.async(self.connection.open_channel(), loop=self.loop)
        self.tick()

    def it_should_send_a_channel_open_frame(self):
        expected = spec.ChannelOpen('')
        self.protocol.send_method.assert_called_once_with(1, expected)


class WhenChannelOpenOKArrives(OpenConnectionContext):
    def given_the_user_has_called_open_channel(self):
        self.task = asyncio.async(self.connection.open_channel())
        self.tick()

    def when_channel_open_ok_arrives(self):
        open_ok_frame = frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        self.tick()
        self.result = self.task.result()

    def it_should_have_the_correct_channel_id(self):
        assert self.result.id == 1


class WhenOpeningASecondChannel(OpenChannelContext):
    def when_the_user_opens_another_channel(self):
        self.result = self.open_channel(2)

    def it_should_send_another_channel_open_frame(self):
        expected = spec.ChannelOpen('')
        self.protocol.send_method.assert_called_once_with(2, expected)

    def it_should_have_the_correct_channel_id(self):
        assert self.result.id == 2


class WhenTheApplicationClosesAChannel(OpenChannelContext):
    def when_I_close_the_channel(self):
        asyncio.async(self.channel.close())
        self.tick()

    def it_should_send_ChannelClose(self):
        self.protocol.send_method.assert_called_once_with(1, spec.ChannelClose(0, 'Channel closed by application', 0, 0))


class WhenTheServerClosesAChannel(OpenChannelContext):
    def when_the_server_shuts_the_channel_down(self):
        channel_close_frame = frames.MethodFrame(self.channel.id, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(channel_close_frame)
        self.tick()

    def it_should_send_ChannelCloseOK(self):
        self.protocol.send_method.assert_called_once_with(self.channel.id, spec.ChannelCloseOK())


class WhenAnotherMethodArrivesAfterIClosedTheChannel(OpenChannelContext):
    def given_that_i_closed_the_channel(self):
        asyncio.async(self.channel.close())
        self.tick()
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = frames.MethodFrame(self.channel.id, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)

    def it_MUST_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenAnotherMethodArrivesAfterTheServerClosedTheChannel(OpenChannelContext):
    def given_the_server_closed_the_channel(self):
        channel_close_frame = frames.MethodFrame(self.channel.id, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(channel_close_frame)
        self.tick()
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = frames.MethodFrame(self.channel.id, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)

    def it_MUST_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenAnAsyncMethodArrivesWhileWeAwaitASynchronousOne(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        self.task = asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()
        self.protocol.reset_mock()

    def when_an_async_method_arrives(self):
        frame = frames.MethodFrame(self.channel.id, spec.BasicDeliver('consumer', 2, False, 'exchange', 'routing_key'))
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_not_close_the_channel(self):
        assert not self.protocol.send_method.called

    def it_should_not_throw_an_exception(self):
        assert not self.task.done()


class WhenAnUnexpectedChannelCloseArrives(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()
        self.protocol.reset_mock()

    def when_ChannelClose_arrives(self):
        frame = frames.MethodFrame(self.channel.id, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_send_ChannelCloseOK(self):
        self.protocol.send_method.assert_called_once_with(1, spec.ChannelCloseOK())


class WhenSettingQOS(OpenChannelContext):
    def when_we_are_setting_prefetch_count_only(self):
        asyncio.async(self.channel.set_qos(prefetch_size=1000, prefetch_count=100, apply_globally=True))
        self.tick()

    def it_should_send_BasicQos_with_default_values(self):
        self.protocol.send_method.assert_called_once_with(1, spec.BasicQos(1000, 100, True))


class WhenBasicQOSOkArrives(OpenChannelContext):
    def given_we_are_setting_qos_settings(self):
        self.task = asyncio.async(self.channel.set_qos(prefetch_size=1000, prefetch_count=100, apply_globally=True))
        self.tick()

    def when_BasicQosOk_arrives(self):
        frame = frames.MethodFrame(self.channel.id, spec.BasicQosOK())
        self.dispatcher.dispatch(frame)
        self.tick()

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
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.tick()

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()
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
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.tick()

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()
        self.tick()

    def it_should_throw_an_exception(self):
        assert self.exception is not None

    def it_should_set_the_reply_code(self):
        assert self.exception.args == (self.expected_message,)

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(None)


# test that the call to handler.ready() happens at the correct time
class WhenBasicReturnArrivesAfterThrowingTheExceptionOnce(OpenChannelContext):
    def given_I_am_listening_for_asyncio_exceptions(self):
        self.expected_message = asynqp.Message('body')

        self.exception = None
        self.loop.set_exception_handler(lambda l, c: setattr(self, "exception", c["exception"]))

        self.return_msg()  # cause the exception to be thrown
        self.exception = None  # reset self.exception

    def when_BasicReturn_arrives(self):
        self.return_msg()

    def it_should_throw_the_exception_again(self):
        assert self.exception is not None

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(None)

    def return_msg(self):
        method = spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key")
        self.dispatcher.dispatch(frames.MethodFrame(self.channel.id, method))
        self.tick()

        header = message.get_header_payload(self.expected_message, spec.BasicGet.method_type[0])
        self.dispatcher.dispatch(frames.ContentHeaderFrame(self.channel.id, header))
        self.tick()

        body = message.get_frame_payloads(self.expected_message, 100)[0]
        self.dispatcher.dispatch(frames.ContentBodyFrame(self.channel.id, body))
        self.tick()
        self.tick()



class WhenTheHandlerIsNotCallable(OpenChannelContext):
    def when_I_set_the_handler(self):
        self.exception = contexts.catch(self.channel.set_return_handler, "i am not callable")

    def it_should_throw_a_TypeError(self):
        assert isinstance(self.exception, TypeError)
