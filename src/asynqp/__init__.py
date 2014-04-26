import asyncio
import enum
import struct
from .exceptions import AMQPError
from . import methods
from . import serialisation


FRAME_END = b'\xCE'


class Connection(object):
    def __init__(self, reader, writer, username='guest', password='guest', virtual_host='/'):
        self.reader = reader
        self.writer = writer
        self.username = username
        self.password = password
        self.virtual_host = virtual_host

    def write_protocol_header(self):
        self.writer.write(b'AMQP\x00\x00\x09\x01')

    def handle(self, frame):
        method_type = frame.payload.method_type
        getattr(self, 'handle_' + type(frame.payload).__name__)(frame)

    def handle_ConnectionStart(self, frame):
        method = methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN': self.username, 'PASSWORD': self.password}, 'en_US')
        frame = Frame(FrameType.method, 0, method)
        self.write_frame(frame)

    def handle_ConnectionTune(self, frame):
        method = methods.ConnectionTuneOK(1024, 0, 0)
        frame = Frame(FrameType.method, 0, method)
        self.write_frame(frame)

        builder = PayloadBuilder(methods.MethodType.connection_open)
        builder.add_short_string(self.virtual_host)
        builder.add_short_string('')
        builder.add_bit(False)
        frame = Frame(FrameType.method, 0, builder.build())
        self.write_frame(frame)

    def write_frame(self, frame):
        self.writer.write(frame.serialise())

    @asyncio.coroutine
    def read_frame(self):
        frame_header = yield from self.reader.read(7)
        frame_type, channel_id, size = struct.unpack('!BHL', frame_header)
        raw_payload = yield from self.reader.read(size)
        frame_end = yield from self.reader.read(1)
        if frame_end !=  b'\xCE':
            raise AMQPError("Frame end byte was incorrect")
        return create_frame(frame_type, channel_id, raw_payload)


def create_frame(frame_type, channel_id, raw_payload):
    if frame_type == 1:
        payload = methods.create_method(raw_payload)
    return Frame(FrameType(frame_type), channel_id, payload)



class Frame(object):
    def __init__(self, frame_type, channel_id, payload):
        self.frame_type = frame_type
        self.channel_id = channel_id
        self.payload = payload

    def serialise(self):
        payload = self.payload.serialise()
        frame = serialisation.pack_octet(self.frame_type.value)
        frame += serialisation.pack_short(self.channel_id)
        body = self.payload.serialise()
        frame += serialisation.pack_long(len(body)) + body
        frame += FRAME_END  # frame_end
        return frame


class FrameType(enum.Enum):
    method = 1


class PayloadBuilder(object):
    def __init__(self, method_type):
        self.method_type = method_type
        self.body = b''
        self.bits = []
        self.bitcount = 0

    def add_bit(self, b):
        b = 1 if b else 0
        shift = self.bitcount % 8
        if shift == 0:
            self.bits.append(0)
        self.bits[-1] |= (b << shift)
        self.bitcount += 1

    def add_octet(self, number):
        self._flush_bits()
        self.body += serialisation.pack_octet(number)

    def add_short(self, number):
        self._flush_bits()
        self.body += serialisation.pack_short(number)

    def add_long(self, number):
        self._flush_bits()
        self.body += serialisation.pack_long(number)

    def add_short_string(self, string):
        self._flush_bits()
        self.body += serialisation.pack_short_string(string)

    def add_table(self, d):
        self._flush_bits()
        self.body += serialisation.pack_table(d)

    def build(self):
        self._flush_bits()
        return methods.METHOD_TYPES[self.method_type.value](self.body)

    def _flush_bits(self):
        for b in self.bits:
            self.body += struct.pack('B', b)
        self.bits = []
        self.bitcount = 0

