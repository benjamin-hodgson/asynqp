from unittest import mock
import contexts
import asynqp
from asynqp import spec
from asynqp import protocol
from asynqp.exceptions import ConnectionLostError
from .base_contexts import MockDispatcherContext, MockServerContext
from .util import testing_exception_handler


class WhenInitiatingProceedings(MockServerContext):
    def when_i_send_the_protocol_header(self):
        self.protocol.send_protocol_header()

    def it_should_write_the_correct_header(self):
        self.server.should_have_received_bytes(b'AMQP\x00\x00\x09\x01')


class WhenAWholeFrameArrives(MockDispatcherContext):
    def establish_the_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)
        self.protocol.heartbeat_monitor = mock.Mock(spec=protocol.HeartbeatMonitor)

    def because_the_whole_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_the_method(self):
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)

    def it_should_reset_the_heartbeat_timeout(self):
        assert self.protocol.heartbeat_monitor.heartbeat_received.called


class WhenAFrameDoesNotEndInFrameEnd(MockServerContext):
    def establish_the_bad_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCD'

    def because_the_bad_frame_arrives(self):
        self.exception = contexts.catch(self.server.send_bytes, self.raw)

    def it_MUST_close_the_connection(self):
        assert self.transport.closed

    def it_should_raise_an_exception(self):
        assert isinstance(self.exception, asynqp.AMQPError)


class WhenHalfAFrameArrives(MockDispatcherContext):
    @classmethod
    def examples_of_incomplete_frames(cls):
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00'  # cut off half way through the payload
        yield b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00'  # cut off right before the frame end byte
        yield b'\x01\x00'  # cut off before the end of the header

    def because_some_of_a_frame_arrives(self, raw):
        self.protocol.data_received(raw)
        self.tick()

    def it_should_not_dispatch_the_method_yet(self):
        assert not self.dispatcher.dispatch.called


class WhenAFrameArrivesInTwoParts(MockDispatcherContext):
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
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)


class WhenMoreThanAWholeFrameArrives(MockDispatcherContext):
    def establish_the_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00\x00\x00\x00\x00\x05\x00\x0A'
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)

    def because_more_than_a_whole_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_the_method_once(self):
        self.dispatcher.dispatch.assert_called_once_with(self.expected_frame)


class WhenTwoFramesArrive(MockDispatcherContext):
    def establish_the_frame(self):
        self.raw = b'\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE\x01\x00\x00\x00\x00\x00\x05\x00\x0A\x00\x29\x00\xCE'
        method = spec.ConnectionOpenOK('')
        self.expected_frame = asynqp.frames.MethodFrame(0, method)

    def because_more_than_a_whole_frame_arrives(self):
        self.protocol.data_received(self.raw)
        self.tick()

    def it_should_dispatch_the_method_twice(self):
        self.dispatcher.dispatch.assert_has_calls([mock.call(self.expected_frame), mock.call(self.expected_frame)])


class WhenTwoFramesArrivePiecemeal(MockDispatcherContext):
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
        self.dispatcher.dispatch.assert_has_calls([mock.call(self.expected_frame), mock.call(self.expected_frame)])


class WhenTheConnectionIsLost(MockServerContext):
    def given_an_exception_handler(self):
        self.connection_lost_error_raised = False
        self.loop.set_exception_handler(self.exception_handler)

    def exception_handler(self, loop, context):
        exception = context.get('exception')
        if type(exception) is ConnectionLostError:
            self.connection_lost_error_raised = True
        else:
            self.loop.default_exception_handler(context)

    def when_the_connection_is_closed(self):
        self.loop.call_soon(self.protocol.connection_lost, Exception)
        self.tick()

    def it_should_raise_a_connection_lost_error(self):
        assert self.connection_lost_error_raised is True

    def cleanup(self):
        self.loop.set_exception_handler(testing_exception_handler)
