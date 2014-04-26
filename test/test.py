import asyncio
import asynqp


class EventLoopContext:
    def given_an_event_loop(self):
        self.loop = asyncio.get_event_loop()


class StreamConnectionContext(EventLoopContext):
    def given_a_socket(self):
        self.reader, self.writer = self.loop.run_until_complete(asyncio.open_connection('localhost', 5672))
        self.connection = asynqp.Connection(self.reader, self.writer, 'guest', 'guest')

    def cleanup_the_socket(self):
        self.writer.close()


class WhenOpeningAConnection(StreamConnectionContext):
    def when_we_write_the_protocol_header(self):
        self.connection.write_protocol_header()
        self.frame = self.loop.run_until_complete(self.connection.read_frame())

    def it_should_return_a_method_frame(self):
        assert self.frame.frame_type == asynqp.FrameType.method

    def it_should_be_communicating_on_the_default_channel(self):
        assert self.frame.channel_id == 0

    def it_should_be_a_start_method(self):
        assert self.frame.payload.method_type == asynqp.MethodType.connection_start


class WhenRespondingToConnectionStart(StreamConnectionContext):
    def given_a_start_frame_from_the_server(self):
        self.connection.write_protocol_header()
        self.start_frame = self.loop.run_until_complete(self.connection.read_frame())

    def because_the_start_frame_arrives(self):
        self.connection.handle(self.start_frame)  # TODO: what does the server do when the credentials are wrong?
        self.response = self.loop.run_until_complete(self.connection.read_frame())

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
        self.connection.handle(self.tune_frame)
        self.response = self.loop.run_until_complete(self.connection.read_frame())

    def it_should_return_open_ok(self):
        assert self.response.payload.method_type == asynqp.MethodType.connection_open_ok

    @asyncio.coroutine
    def get_tune_frame(self):
        self.connection.write_protocol_header()
        start_frame = yield from self.connection.read_frame()
        self.connection.handle(start_frame)
        tune_frame = yield from self.connection.read_frame()
        return tune_frame
