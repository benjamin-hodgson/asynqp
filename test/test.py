import asyncio
import struct
from io import BytesIO
from unittest import mock
import asynqp


class WhenReadingAMethodFrame:
    def given_a_frame_to_read(self):
        self.loop = asyncio.get_event_loop()
        self.reader, self.writer = self.loop.run_until_complete(asyncio.open_connection('localhost', 5672))
        self.frame_reader = asynqp.AMQPFrameReader(self.reader)

        asynqp.write_protocol_header(self.writer)  # the server will return a Start method

    def when_we_read_a_frame(self):
        self.frame = self.loop.run_until_complete(self.frame_reader.read_frame())

    def it_should_return_a_method_frame(self):
        assert self.frame.frame_type == asynqp.FrameType.method

    def it_should_be_communicating_on_the_default_channel(self):
        assert self.frame.channel_id == 0

    def it_should_be_a_start_method(self):
        assert self.frame.payload.method == asynqp.Method.start

