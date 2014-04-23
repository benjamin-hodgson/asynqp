import asyncio
import struct
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
    def given_that_the_server_is_awaiting_start_ok(self):
        asynqp.write_protocol_header(self.writer)
        self.loop.run_until_complete(asynqp.read_frame(self.reader))

    def because_we_write_a_start_ok_frame(self):
        frame = self.make_start_ok_message()
        self.writer.write(frame)
        self.frame = self.loop.run_until_complete(asynqp.read_frame(self.reader))

    def it_should_return_a_method_frame(self):
        assert self.frame.frame_type == asynqp.FrameType.method

    def it_should_be_communicating_on_the_default_channel(self):
        assert self.frame.channel_id == 0

    def it_should_be_a_tune_method(self):
        assert self.frame.payload.method_type == asynqp.MethodType.connection_tune

    def make_start_ok_message(self):
        builder = asynqp.FrameBuilder()

        method = struct.pack('!HH', 10, 11)  # start_ok
        builder.add_bytes_to_payload(method)

        client_properties = {}
        builder.add_table_to_payload(client_properties)

        mechanism = b'AMQPLAIN'
        builder.add_short_string_to_payload(mechanism)

        security_response = {b'LOGIN':b'guest', b'PASSWORD': b'guest'}
        builder.add_table_to_payload(security_response)

        locale = b'en_US'
        builder.add_short_string_to_payload(locale)

        return builder.build()
