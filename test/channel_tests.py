import asyncio
import asynqp
from asyncio import test_utils
from asynqp import spec
from .base_contexts import OpenConnectionContext


class OpenChannelContext(OpenConnectionContext):
    def given_an_open_channel(self):
        self.channel = self.open_channel()
        self.protocol.reset_mock()

    def open_channel(self, channel_id=1):
        task = asyncio.Task(self.connection.open_channel(), loop=self.loop)
        test_utils.run_briefly(self.loop)
        open_ok_frame = asynqp.frames.MethodFrame(channel_id, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        test_utils.run_briefly(self.loop)
        return task.result()


class WhenOpeningAChannel(OpenConnectionContext):
    def when_the_user_wants_to_open_a_channel(self):
        asyncio.Task(self.connection.open_channel(), loop=self.loop)
        test_utils.run_briefly(self.loop)

    def it_should_send_a_channel_open_frame(self):
        expected = spec.ChannelOpen('')
        self.protocol.send_method.assert_called_once_with(1, expected)


class WhenChannelOpenOKArrives(OpenConnectionContext):
    def given_the_user_has_called_open_channel(self):
        self.task = asyncio.Task(self.connection.open_channel())
        test_utils.run_briefly(self.loop)

    def when_channel_open_ok_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)
        test_utils.run_briefly(self.loop)
        self.result = self.task.result()

    def it_should_have_the_correct_channel_id(self):
        assert self.result.channel_id == 1


class WhenOpeningASecondChannel(OpenChannelContext):
    def when_the_user_opens_another_channel(self):
        self.result = self.open_channel(2)

    def it_should_send_another_channel_open_frame(self):
        expected = spec.ChannelOpen('')
        self.protocol.send_method.assert_called_once_with(2, expected)

    def it_should_have_the_correct_channel_id(self):
        assert self.result.channel_id == 2


class WhenTheApplicationClosesAChannel(OpenChannelContext):
    def when_I_close_the_channel(self):
        asyncio.Task(self.channel.close())
        test_utils.run_briefly(self.loop)

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
        asyncio.Task(self.channel.close())
        test_utils.run_briefly(self.loop)
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
        test_utils.run_briefly(self.loop)
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelOpenOK(''))
        self.dispatcher.dispatch(open_ok_frame)

    def it_MUST_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenChannelCloseOKArrives(OpenChannelContext):
    def given_the_user_has_called_close(self):
        self.task = asyncio.Task(self.channel.close())
        test_utils.run_briefly(self.loop)

    def when_channel_close_ok_arrives(self):
        close_ok_frame = asynqp.frames.MethodFrame(1, spec.ChannelCloseOK())
        self.dispatcher.dispatch(close_ok_frame)
        test_utils.run_briefly(self.loop)

    def it_should_close_the_channel(self):
        assert self.channel.closed.done()
