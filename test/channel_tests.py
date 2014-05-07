import asyncio
import asynqp
from asynqp import spec
from . import util
from .base_contexts import OpenConnectionContext, OpenChannelContext


class WhenOpeningAChannel(OpenConnectionContext):
    def when_the_user_wants_to_open_a_channel(self):
        asyncio.async(self.connection.open_channel(), loop=self.loop)
        self.go()

    def it_should_send_a_channel_open_frame(self):
        expected = spec.ChannelOpen('')
        self.protocol.send_method.assert_called_once_with(1, expected)


class WhenChannelOpenOKArrives(OpenConnectionContext):
    def given_the_user_has_called_open_channel(self):
        self.task = asyncio.async(self.connection.open_channel())
        self.go()

    def when_channel_open_ok_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        self.go()
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
        self.go()

    def it_should_send_ChannelClose(self):
        self.protocol.send_method.assert_called_once_with(1, spec.ChannelClose(0, 'Channel closed by application', 0, 0))


class WhenTheServerClosesAChannel(OpenChannelContext):
    def when_the_server_shuts_the_channel_down(self):
        channel_close_frame = asynqp.frames.MethodFrame(1, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(channel_close_frame)

    def it_should_send_ChannelCloseOK(self):
        self.protocol.send_method.assert_called_once_with(1, spec.ChannelCloseOK())


class WhenAnotherMethodArrivesAfterIClosedTheChannel(OpenChannelContext):
    def given_that_i_closed_the_channel(self):
        asyncio.async(self.channel.close())
        self.go()
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
        self.go()
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)

    def it_MUST_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenAnUnexpectedSynchronousMethodArrives(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        self.task = asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.go()
        self.protocol.reset_mock()

    def when_the_wrong_method_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        self.go()

    def it_should_close_the_channel(self):
        self.protocol.send_method.assert_called_once_with(1, util.any(spec.ChannelClose))

    def it_should_throw(self):
        assert isinstance(self.task.exception(), asynqp.AMQPError)


class WhenAnAsyncMethodArrivesWhileWeAwaitASynchronousOne(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        self.task = asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.go()
        self.protocol.reset_mock()

    def when_an_async_method_arrives(self):
        frame = asynqp.frames.MethodFrame(1, spec.BasicDeliver('consumer', 2, False, 'exchange', 'routing_key'))
        self.dispatcher.dispatch(frame)

    def it_should_not_close_the_channel(self):
        assert not self.protocol.send_method.called

    def it_should_not_throw_an_exception(self):
        assert not self.task.done()


class WhenAnUnexpectedChannelCloseArrives(OpenChannelContext):
    def given_we_are_awaiting_QueueDeclareOK(self):
        asyncio.async(self.channel.declare_queue('my.nice.queue', durable=True, exclusive=True, auto_delete=True), loop=self.loop)
        self.go()
        self.protocol.reset_mock()

    def when_ChannelClose_arrives(self):
        frame = asynqp.frames.MethodFrame(1, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.dispatcher.dispatch(frame)

    def it_should_send_ChannelCloseOK(self):
        self.protocol.send_method.assert_called_once_with(1, spec.ChannelCloseOK())
