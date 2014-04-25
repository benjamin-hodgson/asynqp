import asyncio
import asynqp


class EventLoopContext:
    def given_an_event_loop(self):
        self.loop = asyncio.get_event_loop()


class StreamConnectionContext(EventLoopContext):
    def given_a_socket(self):
        self.reader, self.writer = self.loop.run_until_complete(asyncio.open_connection('localhost', 5672))

    def cleanup_the_socket(self):
        self.writer.close()


class WhenOpeningAConnection(StreamConnectionContext):
    def when_we_write_the_protocol_header(self):
        asynqp.write_protocol_header(self.writer)
        self.frame = self.loop.run_until_complete(asynqp.read_frame(self.reader))

    def it_should_return_a_method_frame(self):
        assert self.frame.frame_type == asynqp.FrameType.method

    def it_should_be_communicating_on_the_default_channel(self):
        assert self.frame.channel_id == 0

    def it_should_be_a_start_method(self):
        assert self.frame.payload.method_type == asynqp.MethodType.connection_start


class WhenRespondingToConnectionStart(StreamConnectionContext):
    def given_a_start_frame_from_the_server(self):
        asynqp.write_protocol_header(self.writer)
        self.start_frame = self.loop.run_until_complete(asynqp.read_frame(self.reader))

    def because_the_start_frame_arrives(self):
        asynqp.handle_connection_start(self.writer, self.start_frame, 'guest', 'guest')  # TODO: what does the server do when the credentials are wrong?
        self.response = self.loop.run_until_complete(asynqp.read_frame(self.reader))

    def it_should_return_a_method_frame(self):
        assert self.response.frame_type == asynqp.FrameType.method

    def it_should_be_communicating_on_the_default_channel(self):
        assert self.response.channel_id == 0

    def it_should_be_a_tune_method(self):
        assert self.response.payload.method_type == asynqp.MethodType.connection_tune


class WhenRespondingToConnectionTune(StreamConnectionContext):
    def given_a_tune_frame_from_the_server(self):
        self.tune_frame = self.loop.run_until_complete(self.get_tune_frame())

    def when_the_tune_frame_arrives(self):
        asynqp.handle_tune(self.writer, self.tune_frame)
        self.response = self.loop.run_until_complete(asyncio.wait_for(asynqp.read_frame(self.reader), 0.1))

    def it_should_return_open_ok(self):
        assert self.response.payload.method_type == asynqp.MethodType.connection_open_ok

    @asyncio.coroutine
    def get_tune_frame(self):
        asynqp.write_protocol_header(self.writer)
        start_frame = yield from asynqp.read_frame(self.reader)
        asynqp.handle_connection_start(self.writer, start_frame, 'guest', 'guest')
        tune_frame = yield from asynqp.read_frame(self.reader)
        return tune_frame
