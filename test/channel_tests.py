import contexts
import asyncio
import asynqp
from asynqp import spec
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
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
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
        channel_close_frame = asynqp.frames.MethodFrame(1, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(channel_close_frame)
        self.tick()

    def it_should_send_ChannelCloseOK(self):
        self.protocol.send_method.assert_called_once_with(1, spec.ChannelCloseOK())


class WhenAnotherMethodArrivesAfterIClosedTheChannel(OpenChannelContext):
    def given_that_i_closed_the_channel(self):
        asyncio.async(self.channel.close())
        self.tick()
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)

    def it_MUST_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenAnotherMethodArrivesAfterTheServerClosedTheChannel(OpenChannelContext):
    def given_the_server_closed_the_channel(self):
        channel_close_frame = asynqp.frames.MethodFrame(1, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(channel_close_frame)
        self.tick()
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)

    def it_MUST_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenAnAsyncMethodArrivesWhileWeAwaitASynchronousOne(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        self.task = asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.tick()
        self.protocol.reset_mock()

    def when_an_async_method_arrives(self):
        frame = asynqp.frames.MethodFrame(1, spec.BasicDeliver('consumer', 2, False, 'exchange', 'routing_key'))
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
        frame = asynqp.frames.MethodFrame(1, spec.ChannelClose(123, 'i am tired of you', 40, 50))
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
        frame = asynqp.frames.MethodFrame(1, spec.BasicQosOK())
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_yield_result(self):
        assert self.task.done()


class WhenBasicReturnArrivesAndIHaveNotDefinedAHandler(OpenChannelContext):
    def given_I_am_listening_for_exceptions(self):
        self.exception = None
        self.loop.set_exception_handler(lambda l, c: setattr(self, "exception", c["exception"]))

    def when_BasicReturn_arrives(self):
        frame = asynqp.frames.MethodFrame(1, spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key"))
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_throw_an_exception(self):
        assert self.exception is not None

    def it_should_set_the_reply_code(self):
        assert self.exception.reply_code == 123

    def it_should_set_the_message(self):
        assert self.exception.message == "you messed up"

    def it_should_set_the_exchange_name(self):
        assert self.exception.exchange_name == "the.exchange"

    def it_should_set_the_routing_key(self):
        assert self.exception.routing_key == "the.routing.key"

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(None)


class WhenAnotherFrameArrivesAfterBasicReturn(OpenChannelContext):
    def given_BasicReturn_has_arrived(self):
        self.loop.set_exception_handler(lambda l, c: None)

        frame = asynqp.frames.MethodFrame(1, spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key"))
        self.dispatcher.dispatch(frame)
        self.tick()

    def when_another_frame_arrives(self):  # just use another BasicReturn
        self.exception = None
        self.loop.set_exception_handler(lambda l, c: setattr(self, "exception", c["exception"]))

        frame = asynqp.frames.MethodFrame(1, spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key"))
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_handle_the_frame_correctly(self):
        assert self.exception is not None

    def cleanup_the_exception_handler(self):
        self.loop.set_exception_handler(None)


class WhenBasicReturnArrivesAndIHaveDefinedAHandler(OpenChannelContext):
    def given_I_have_set_a_BasicReturn_handler(self):
        self.channel.set_return_handler(lambda x: setattr(self, "details", x))

    def when_BasicReturn_arrives(self):
        frame = asynqp.frames.MethodFrame(1, spec.BasicReturn(123, "you messed up", "the.exchange", "the.routing.key"))
        self.dispatcher.dispatch(frame)
        self.tick()

    def it_should_call_the_handler_with_a_dict_containing_the_info(self):
        assert self.details == {"reply_code": 123,
                                "message": "you messed up",
                                "exchange_name": "the.exchange",
                                "routing_key": "the.routing.key"}

