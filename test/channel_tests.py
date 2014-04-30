import asynqp
from asynqp import spec
from .base_contexts import OpenConnectionContext


class OpenChannelContext(OpenConnectionContext):
    def given_an_open_channel(self):
        self.channel = self.open_channel()
        self.protocol.reset_mock()

    def open_channel(self, channel_id=1):
        coro = self.connection.open_channel()
        next(coro)
        open_ok_frame = asynqp.MethodFrame(channel_id, spec.ChannelOpenOK(''))
        self.connection.dispatch(open_ok_frame)
        try:
            next(coro)
        except StopIteration as e:
            return e.value


class WhenOpeningAChannel(OpenConnectionContext):
    def when_the_user_wants_to_open_a_channel(self):
        next(self.connection.open_channel())

    def it_should_send_a_channel_open_frame(self):
        expected_frame = asynqp.MethodFrame(1, spec.ChannelOpen(''))
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenChannelOpenOKArrives(OpenConnectionContext):
    def given_the_user_has_called_open_channel(self):
        self.coro = self.connection.open_channel()
        next(self.coro)

    def when_channel_open_ok_arrives(self):
        open_ok_frame = asynqp.MethodFrame(1, spec.ChannelOpenOK(''))
        self.connection.dispatch(open_ok_frame)
        try:
            next(self.coro)
        except StopIteration as e:
            self.result = e.value

    def it_should_have_the_correct_channel_id(self):
        assert self.result.channel_id == 1


class WhenOpeningASecondChannel(OpenChannelContext):
    def when_the_user_opens_another_channel(self):
        self.result = self.open_channel(2)

    def it_should_send_another_channel_open_frame(self):
        expected_frame = asynqp.MethodFrame(2, spec.ChannelOpen(''))
        self.protocol.send_frame.assert_called_once_with(expected_frame)

    def it_should_have_the_correct_channel_id(self):
        assert self.result.channel_id == 2


class WhenClosingAChannel(OpenChannelContext):
    def when_I_close_the_channel(self):
        next(self.channel.close())

    def it_should_send_a_ChannelClose_method(self):
        expected_frame = asynqp.MethodFrame(1, spec.ChannelClose(0, 'Channel closed by application', 0, 0))
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenTheServerClosesAChannel(OpenChannelContext):
    def when_the_server_shuts_the_channel_down(self):
        channel_close_frame = asynqp.MethodFrame(1, spec.ChannelClose(123, 'i am tired of you', 40, 50))
        self.connection.dispatch(channel_close_frame)

    def it_should_send_ChannelCloseOK(self):
        expected_frame = asynqp.MethodFrame(1, spec.ChannelCloseOK())
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenAnotherMethodArrivesAfterTheChannelIsClosed(OpenChannelContext):
    def given_that_i_closed_the_channel(self):
        next(self.channel.close())
        self.protocol.reset_mock()

    def when_another_method_arrives(self):
        open_ok_frame = asynqp.MethodFrame(1, spec.ChannelOpenOK(''))
        self.connection.dispatch(open_ok_frame)

    def it_should_discard_the_method(self):
        assert not self.protocol.send_frame.called


class WhenChannelCloseOKArrives(OpenChannelContext):
    def given_the_user_has_called_close(self):
        self.coro = self.channel.close()
        next(self.coro)

    def when_channel_close_ok_arrives(self):
        close_ok_frame = asynqp.MethodFrame(1, spec.ChannelCloseOK())
        self.connection.dispatch(close_ok_frame)

    def it_should_close_the_channel(self):
        assert self.channel.closed.result()
