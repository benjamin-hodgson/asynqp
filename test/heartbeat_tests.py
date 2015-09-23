import asyncio
from asynqp import spec
from asynqp.frames import HeartbeatFrame
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
        self.loop.run_until_complete(asyncio.sleep(0.021))

    def it_should_close_the_connection(self):
        self.server.should_have_received_method(0, spec.ConnectionClose(501, 'Heartbeat timed out', 0, 0))

    def cleanup_tasks(self):
        self.protocol.heartbeat_monitor.stop()
        self.loop.run_until_complete(
            asyncio.wait_for(self.protocol.heartbeat_monitor.wait_closed(),
                             timeout=0.2))
