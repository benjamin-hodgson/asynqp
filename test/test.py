import asyncio
import struct
from io import BytesIO
from unittest import mock
import asynqp


class WhenOpeningAConnection:
    def given_a_socket(self):
        self.loop = asyncio.get_event_loop()
        self.reader, self.writer = self.loop.run_until_complete(asyncio.open_connection('localhost', 5672))
        self.frame_reader = asynqp.AMQPFrameReader(self.reader)

    def when_we_write_the_protocol_header(self):
        asynqp.write_protocol_header(self.writer)
        self.frame = self.loop.run_until_complete(self.frame_reader.read_frame())

    def it_should_return_a_method_frame(self):
        assert self.frame.frame_type == asynqp.FrameType.method

    def it_should_be_communicating_on_the_default_channel(self):
        assert self.frame.channel_id == 0

    def it_should_be_a_start_method(self):
        assert self.frame.payload.method == asynqp.Method.start


class WhenWritingStartOk:
    def given_a_socket(self):
        self.loop = asyncio.get_event_loop()
        self.reader, self.writer = self.loop.run_until_complete(asyncio.open_connection('localhost', 5672))

    def because_we_write_a_start_ok_frame(self):
        # write the frame header

        self.writer.write(b'\x01')  # frame is a method
        self.writer.write(b'\x00\x00')  # channel 0
        self.writer.write(b'???')  # payload size in bytes

        # write the method

        self.writer.write(b'\x00\x0A\x00\x0B')  # (10, 11) - start_ok

        # write the client-properties table
        self.writer.write(b'???')  # length of the 'long string' containing the table as a 4-byte unsigned int
        self.writer.write(b'???')  # the 'long string' containing the client-properties table

        # write the security mechanism
        self.writer.write(b'???')  # length of the 'short string' containing the security mechanism as a 1-byte unsigned int
        self.writer.write(b'???')  # the 'short string' containing the security mechanism

        # write the security response
        self.writer.write(b'???')  # length of the 'long string' containing the response as a 4-byte unsigned int
        self.writer.write(b'???')  # the 'long string' containing the security response

        # write the locale
        self.writer.write(b'???')  # length of the 'short string' containing the locale as a 1-byte unsigned int
        self.writer.write(b'???')  # the 'short string' containing the locale

        # write the frame end byte

        self.writer.write(b'\xCE')  # frame_end
