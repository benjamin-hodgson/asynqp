from unittest import mock
import asynqp
from asynqp import protocol
from .base_contexts import ProtocolContext, MockLoopContext


class WhenStartingTheHeartbeat(ProtocolContext, MockLoopContext):
    def when_I_start_the_heartbeat(self):
        self.protocol.start_heartbeat(5)

    def it_should_set_up_heartbeat_and_timeout_callbacks(self):
        assert self.loop.call_later.call_args_list == [
            mock.call(5, self.protocol.heartbeat_monitor.send_heartbeat),
            mock.call(10, self.protocol.heartbeat_monitor.heartbeat_timed_out)
        ]


class WhenHeartbeatIsDisabled(ProtocolContext, MockLoopContext):
    def given_the_server_does_not_want_a_heartbeat(self):
        self.heartbeat_interval = 0

    def when_I_start_the_heartbeat(self):
        self.protocol.start_heartbeat(self.heartbeat_interval)

    def it_should_not_set_up_callbacks(self):
        assert not self.loop.call_later.called


class WhenItIsTimeToHeartbeat(MockLoopContext):
    def given_a_heartbeat_monitor(self):
        self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.heartbeat_monitor = protocol.HeartbeatMonitor(self.protocol, self.loop, 5)

    def when_the_event_loop_comes_a_knockin(self):
        self.heartbeat_monitor.send_heartbeat()

    def it_should_send_a_heartbeat_frame(self):
        self.protocol.send_frame.assert_called_once_with(asynqp.frames.HeartbeatFrame())

    def it_should_set_up_the_next_heartbeat(self):
        self.loop.call_later.assert_called_once_with(5, self.heartbeat_monitor.send_heartbeat)


class WhenResettingTheHeartbeatTimeout(MockLoopContext):
    def given_a_heartbeat_monitor(self):
        self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.heartbeat_monitor = protocol.HeartbeatMonitor(self.protocol, self.loop, 5)
        self.heartbeat_monitor.monitor_heartbeat()
        self.loop.reset_mock()

    def because_the_timeout_gets_reset(self):
        self.heartbeat_monitor.heartbeat_received()

    def it_should_cancel_the_close_callback(self):
        self.loop.call_later.return_value.cancel.assert_called_once_with()

    def it_should_set_up_another_close_callback(self):
        self.loop.call_later.assert_called_once_with(10, self.heartbeat_monitor.heartbeat_timed_out)


class WhenTheHeartbeatTimesOut(MockLoopContext):
    def given_a_heartbeat_monitor(self):
        self.protocol = mock.Mock(spec=asynqp.AMQP)
        self.heartbeat_monitor = protocol.HeartbeatMonitor(self.protocol, self.loop, 5)

    def when_the_heartbeat_times_out(self):
        self.heartbeat_monitor.heartbeat_timed_out()

    def it_should_send_connection_close(self):
        self.protocol.send_method.assert_called_once_with(0, asynqp.spec.ConnectionClose(501, 'Heartbeat timed out', 0, 0))
