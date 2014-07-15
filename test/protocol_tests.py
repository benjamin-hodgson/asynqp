from unittest import mock
import contexts
import asynqp
from asynqp import spec
from asynqp import protocol
from .base_contexts import ProtocolContext, MockHandlerContext


class WhenInitiatingProceedings(ProtocolContext):
    def when_i_send_the_protocol_header(self):
        self.protocol.send_protocol_header()

    def it_should_write_the_correct_header(self):
        self.transport.write.assert_called_once_with(b'AMQP\x00\x00\x09\x01')


class WhenAWholeFrameArrives(MockHandlerContext(0)):
    def establish_the_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)
        self.protocol.heartbeat_monitor = mock.Mock(spec=protocol.HeartbeatMonitor)

    def because_the_whole_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_the_method(self):
        self.handler.handle.assert_called_once_with(self.expected_frame)

    def it_should_reset_the_heartbeat_timeout(self):
        assert self.protocol.heartbeat_monitor.heartbeat_received.called


class WhenAFrameDoesNotEndInFrameEnd(ProtocolContext):
    def establish_the_bad_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCD'

    def because_the_bad_frame_arrives(self):
        self.exception = contexts.catch(self.protocol.data_received, self.raw)

    def it_MUST_close_the_connection(self):
        assert self.transport.close.called

    def it_should_raise_an_exception(self):
        assert isinstance(self.exception, asynqp.AMQPError)


class WhenHalfAFrameArrives(MockHandlerContext(0)):
    @classmethod
    def examples_of_incomplete_frames(cls):
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00'  # cut off half way through the payload
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00'  # cut off right before the frame end byte
        yield b'\x01\x00'  # cut off before the end of the header

    def because_the_whole_frame_arrives(self, raw):
        self.protocol.data_received(raw)
        self.tick()

    def it_should_not_dispatch_the_method(self):
        assert not self.handler.handle.called


class WhenAFrameArrivesInTwoParts(MockHandlerContext(0)):
    @classmethod
    def examples_of_broken_up_frames(cls):
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A', b'\x00\x29\x00\xCE'  # cut off half way through the payload
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00', b'\xCE'  # cut off right before the frame end byte
        yield b'\x01\x00', b'\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'  # cut off before the end of the header

    def establish_the_frame(self):
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)

    def because_the_whole_frame_eventually_arrives(self, raw1, raw2):
        self.protocol.data_received(raw1)
        self.tick()
        self.protocol.data_received(raw2)
        self.tick()

    def it_should_dispatch_the_method(self):
        self.handler.handle.assert_called_once_with(self.expected_frame)


class WhenMoreThanAWholeFrameArrives(MockHandlerContext(0)):
    def establish_the_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00\x00\x00\x00\x00\x05\x00\x0A'
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)

    def because_more_than_a_whole_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_the_method_once(self):
        self.handler.handle.assert_called_once_with(self.expected_frame)


class WhenTwoFramesArrive(MockHandlerContext(0)):
    def establish_the_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)

    def because_more_than_a_whole_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_the_method_twice(self):
        self.handler.handle.assert_has_calls([mock.call(self.expected_frame), mock.call(self.expected_frame)])


class WhenTwoFramesArrivePiecemeal(MockHandlerContext(0)):
    @classmethod
    def examples_of_broken_up_frames(cls):
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE', b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00', b'\x29\x00\xCE'
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00', b'\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        yield b'\x01', b'\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        yield b'\x01', b'\x00\x00\x00\x00\x00\x05\x00', b'\x0A\x00\x29\x00', b'\xCE\x01\x00\x00\x00\x00\x00\x05\x00', b'\x0A\x00\x29\x00\xCE', b''

    def establish_what_we_expected(self):
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)

    def because_two_frames_arrive_in_bits(self, fragments):
        for fragment in fragments:
            self.protocol.data_received(fragment)
            self.tick()

    def it_should_dispatch_the_method_twice(self):
        self.handler.handle.assert_has_calls([mock.call(self.expected_frame), mock.call(self.expected_frame)])
