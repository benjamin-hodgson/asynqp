import asynqp
from asynqp import methods
from .base_contexts import LoopContext, OpenConnectionContext


class WhenOpeningAChannel(OpenConnectionContext, LoopContext):
    def when_the_user_wants_to_open_a_channel(self):
        next(self.connection.open_channel())

    def it_should_send_a_channel_open_frame(self):
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 1, methods.ChannelOpen(''))
        self.protocol.send_frame.assert_called_once_with(expected_frame)


class WhenChannelOpenOKArrives(OpenConnectionContext, LoopContext):
    def given_the_user_has_called_open_channel(self):
        self.coro = self.connection.open_channel()
        next(self.coro)

    def when_channel_open_ok_arrives(self):
        open_ok_frame = asynqp.Frame(asynqp.FrameType.method, 1, methods.ChannelOpenOK(''))
        self.connection.handle(open_ok_frame)
        try:
            next(self.coro)
        except StopIteration as e:
            self.result = e.value

    def it_should_have_the_correct_channel_id(self):
        assert self.result.channel_id == 1


class WhenOpeningASecondChannel(OpenConnectionContext, LoopContext):
    def given_an_open_connection_with_an_open_channel(self):
        coro = self.connection.open_channel()
        next(coro)
        open_ok_frame = asynqp.Frame(asynqp.FrameType.method, 1, methods.ChannelOpenOK(''))
        self.connection.handle(open_ok_frame)
        try:
            next(coro)
        except StopIteration as e:
            pass
        self.protocol.reset_mock()

    def when_the_user_wants_to_open_another_channel(self):
        self.coro = self.connection.open_channel()
        next(self.coro)
        open_ok_frame = asynqp.Frame(asynqp.FrameType.method, 2, methods.ChannelOpenOK(''))
        self.connection.handle(open_ok_frame)
        try:
            next(self.coro)
        except StopIteration as e:
            self.result = e.value

    def it_should_send_another_channel_open_frame(self):
        expected_frame = asynqp.Frame(asynqp.FrameType.method, 2, methods.ChannelOpen(''))
        self.protocol.send_frame.assert_called_once_with(expected_frame)

    def it_should_have_the_correct_channel_id(self):
        assert self.result.channel_id == 2
