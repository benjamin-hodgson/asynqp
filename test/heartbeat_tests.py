import asyncio
from unittest import mock
from asynqp.frames import HeartbeatFrame
from asynqp.exceptions import ConnectionLostError
from .base_contexts import MockServerContext


class WhenServerWaitsForHeartbeat(MockServerContext):
    def when_heartbeating_starts(self):
        self.protocol.start_heartbeat(0.01)
        self.loop.run_until_complete(asyncio.sleep(0.015))

    def it_should_send_the_heartbeat(self):
        self.server.should_have_received_frame(HeartbeatFrame())

    def cleanup_tasks(self):
        self.protocol.heartbeat_monitor.stop()
        self.loop.run_until_complete(
            asyncio.wait_for(self.protocol.heartbeat_monitor.wait_closed(),
                             timeout=0.2))


class WhenServerRespondsToHeartbeat(MockServerContext):
    def given_i_started_heartbeating(self):
        self.protocol.start_heartbeat(0.01)
        self.loop.run_until_complete(asyncio.sleep(0.015))

    def when_the_server_replies(self):
        self.server.send_frame(HeartbeatFrame())
        self.loop.run_until_complete(asyncio.sleep(0.005))

    def it_should_send_the_heartbeat(self):
        self.server.should_have_received_frames([HeartbeatFrame(), HeartbeatFrame()])

    def cleanup_tasks(self):
        self.protocol.heartbeat_monitor.stop()
        self.loop.run_until_complete(
            asyncio.wait_for(self.protocol.heartbeat_monitor.wait_closed(),
                             timeout=0.2))


class WhenServerDoesNotRespondToHeartbeat(MockServerContext):
    def given_i_started_heartbeating(self):
        self.protocol.start_heartbeat(0.01)

    def when_the_server_dies(self):
        with mock.patch("asynqp.routing.Dispatcher.dispatch_all") as mocked:
            self.loop.run_until_complete(asyncio.sleep(0.021))
            self.mocked = mocked

    def it_should_dispatch_a_poison_pill(self):
        assert self.mocked.called
        assert isinstance(
            self.mocked.call_args[0][0].exception, ConnectionLostError)

    def cleanup_tasks(self):
        self.protocol.heartbeat_monitor.stop()
        self.loop.run_until_complete(
            asyncio.wait_for(self.protocol.heartbeat_monitor.wait_closed(),
                             timeout=0.2))
