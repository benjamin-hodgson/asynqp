import asyncio
import enum
import struct


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
        getattr(self, 'handle_' + method_type.name)(frame)

    def handle_connection_start(self, frame):
        builder = PayloadBuilder(MethodType.connection_start_ok)
        builder.add_table({})
        builder.add_short_string('AMQPLAIN')
        builder.add_table({'LOGIN': self.username, 'PASSWORD': self.password})
        builder.add_short_string('en_US')
        frame = Frame(FrameType.method, 0, builder.build())
        self.write_frame(frame)

    def handle_connection_tune(self, frame):
        builder = PayloadBuilder(MethodType.connection_tune_ok)
        builder.add_short(1024)  # maximum channel number
        builder.add_long(0)  # no maximum frame size
        builder.add_short(0)  # no heartbeat
        frame = Frame(FrameType.method, 0, builder.build())
        self.write_frame(frame)

        builder = PayloadBuilder(MethodType.connection_open)
        builder.add_short_string(self.virtual_host)
        builder.add_short_string('')
        builder.add_bit(False)
        frame = Frame(FrameType.method, 0, builder.build())
        self.write_frame(frame)

    def write_frame(self, frame):
        self.writer.write(frame.serialise())

    @asyncio.coroutine
    def read_frame(self):
        bytes = yield from self.reader.read(7)
        frame_type, channel_id, size = struct.unpack('!BHL', bytes)
        raw_payload = yield from self.reader.read(size)
        frame_end = yield from self.reader.read(1)
        if frame_end !=  b'\xCE':
            raise AMQPError("Frame end byte was incorrect")

        method_type = MethodType(struct.unpack('!HH', raw_payload[0:4]))

        return Frame(FrameType(frame_type), channel_id, Payload(method_type, raw_payload[4:]))


class Frame(object):
    def __init__(self, frame_type, channel_id, payload):
        self.frame_type = frame_type
        self.channel_id = channel_id
        self.payload = payload

    def serialise(self):
        payload = self.payload.serialise()
        frame = pack_octet(self.frame_type.value)
        frame += pack_short(self.channel_id)
        frame += self.payload.serialise()
        frame += FRAME_END  # frame_end
        return frame


class Payload(object):
    def __init__(self, method_type, arguments):
        self.method_type = method_type
        self.arguments = arguments

    def serialise(self):
        body = struct.pack('!HH', *self.method_type.value) + self.arguments
        return pack_long(len(body)) + body


class FrameType(enum.Enum):
    method = 1


class MethodType(enum.Enum):
    connection_start = (10, 10)
    connection_start_ok = (10, 11)
    connection_tune = (10, 30)
    connection_tune_ok = (10, 31)
    connection_open = (10, 40)
    connection_open_ok = (10, 41)

class AMQPError(IOError):
    pass


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
        self.body += pack_octet(number)

    def add_short(self, number):
        self._flush_bits()
        self.body += pack_short(number)

    def add_long(self, number):
        self._flush_bits()
        self.body += pack_long(number)

    def add_short_string(self, string):
        self._flush_bits()
        self.body += short_string(string)

    def add_table(self, d):
        self._flush_bits()
        self.body += table(d)

    def build(self):
        self._flush_bits()
        return Payload(self.method_type, self.body)

    def _flush_bits(self):
        for b in self.bits:
            self.body += struct.pack('B', b)
        self.bits = []
        self.bitcount = 0


def short_string(string):
    bytes = string.encode('utf-8')
    return pack_octet(len(bytes)) + bytes

def long_string(string):
    bytes = string.encode('utf-8')
    return pack_long(len(bytes)) + bytes

def table(d):
    bytes = b''
    for key in d:
        bytes += short_string(key)
        bytes += b'S'
        bytes += long_string(d[key])
    return pack_long(len(bytes)) + bytes

def pack_octet(number):
    return struct.pack('!B', number)

def pack_short(number):
    return struct.pack('!H', number)

def pack_long(number):
    return struct.pack('!L', number)
