from io import BytesIO
import contexts
from datetime import datetime, timezone, timedelta
from asynqp import serialisation, AMQPError


class WhenParsingATable:
    @classmethod
    def examples_of_tables(self):
        yield b"\x00\x00\x00\x00", {}
        yield b"\x00\x00\x00\x0E\x04key1t\x00\x04key2t\x01", {'key1': False, 'key2': True}
        yield b"\x00\x00\x00\x06\x03keyb\xff", {'key': -1}
        yield b"\x00\x00\x00\x07\x03keys\xff\xff", {'key': -1}
        yield b"\x00\x00\x00\x09\x03keyI\xff\xff\xff\xff", {'key': -1}
        yield b"\x00\x00\x00\x0C\x03keyl\xff\xff\xff\xff\xff\xff\xff\xff", {'key': -1}
        yield b"\x00\x00\x00\x05\x03keyV", {'key': None}
        yield b"\x00\x00\x00\x05\x03keyA\x00\x00\x00\x00", {'key': []}
        yield b"\x00\x00\x00\x0C\x03keyx\x00\x00\x00\x04\x00\x01\x02\x03", {'key': b"\x00\x01\x02\x03"}
        yield b"\x00\x00\x00\x0E\x03keyS\x00\x00\x00\x05hello", {'key': 'hello'}
        yield b"\x00\x00\x00\x16\x03keyF\x00\x00\x00\x0D\x0Aanotherkeyt\x00", {'key': {'anotherkey': False}}

    def because_we_read_the_table(self, bytes, expected):
        self.result = serialisation.read_table(BytesIO(bytes))

    def it_should_return_the_table(self, bytes, expected):
        assert self.result == expected


class WhenPackingAndUnpackingATable:
    @classmethod
    def examples_of_tables(cls):
        yield {'a': (1 << 16), 'b': (1 << 15)}
        yield {'c': 65535, 'd': -65535}
        yield {'e': -65536}
        yield {'f': -0x7FFFFFFF, 'g': 0x7FFFFFFF}
        yield {'x': b"\x01\x02"}
        yield {'x': []}
        yield {'l': None}
        yield {'l': 1.0}

    def because_we_pack_and_unpack_the_table(self, table):
        self.result = serialisation.read_table(BytesIO(serialisation.pack_table(table)))

    def it_should_return_the_table(self, table):
        assert self.result == table


class WhenParsingAHugeTable:
    @classmethod
    def examples_of_huge_tables(self):
        # That would be -1 for an signed int
        yield b"\xFF\xFF\xFF\xFF\xFF"

    def because_we_read_the_table(self, bytes):
        # We expect the serialisation to read over the bounds, but only if it is unsigned
        self.exception = contexts.catch(serialisation.read_table, BytesIO(bytes))

    def it_should_throw_an_AMQPError(self):
        assert isinstance(self.exception, AMQPError)


class WhenParsingABadTable:
    @classmethod
    def examples_of_bad_tables(self):
        yield b"\x00\x00\x00\x0F\x04key1t\x00\x04key2t\x01"  # length too long
        yield b"\x00\x00\x00\x06\x04key1X"  # bad value type code

    def because_we_read_the_table(self, bytes):
        self.exception = contexts.catch(serialisation.read_table, BytesIO(bytes))

    def it_should_throw_an_AMQPError(self):
        assert isinstance(self.exception, AMQPError)


class WhenParsingAnArray:
    @classmethod
    def examples_of_arrays(self):
        yield b"\x00\x00\x00\x00", []
        yield b"\x00\x00\x00\x04t\x00t\x01", [False, True]
        yield b"\x00\x00\x00\x02b\xff", [-1]
        yield b"\x00\x00\x00\x03s\xff\xff", [-1]
        yield b"\x00\x00\x00\x05I\xff\xff\xff\xff", [-1]
        yield b"\x00\x00\x00\x09l\xff\xff\xff\xff\xff\xff\xff\xff", [-1]
        yield b"\x00\x00\x00\x01V", [None]
        yield b"\x00\x00\x00\x05A\x00\x00\x00\x00", [[]]
        yield b"\x00\x00\x00\x09x\x00\x00\x00\x04\x00\x01\x02\x03", [b"\x00\x01\x02\x03"]
        yield b"\x00\x00\x00\x0AS\x00\x00\x00\x05hello", ['hello']
        yield b"\x00\x00\x00\x12F\x00\x00\x00\x0D\x0Aanotherkeyt\x00", [{'anotherkey': False}]

    def because_we_read_the_array(self, buffer, expected):
        self.result = serialisation.read_array(BytesIO(buffer))

    def it_should_return_the_array(self, buffer, expected):
        assert self.result == expected


class WhenParsingALongString:
    def because_we_read_a_long_string(self):
        self.result = serialisation.read_long_string(BytesIO(b"\x00\x00\x00\x05hello"))

    def it_should_return_the_string(self):
        assert self.result == 'hello'


class WhenParsingABadLongString:
    def because_we_read_a_bad_long_string(self):
        self.exception = contexts.catch(serialisation.read_long_string, BytesIO(b"\x00\x00\x00\x10hello"))  # length too long

    def it_should_throw_an_AMQPError(self):
        assert isinstance(self.exception, AMQPError)


class WhenPackingBools:
    @classmethod
    def examples_of_bools(self):
        yield [False], b"\x00"
        yield [True], b"\x01"
        yield [True, False, True], b'\x05'
        yield [True, False], b'\x01'
        yield [True, True, True, True, True, True, True, True], b'\xFF'

    def because_I_pack_them(self, bools, expected):
        self.result = serialisation.pack_bools(*bools)

    def it_should_pack_them_correctly(self, bools, expected):
        assert self.result == expected


class WhenParsingATimestamp:
    @classmethod
    def examples_of_timestamps(cls):
        # The timestamp should be zero relative to epoch
        yield b'\x00\x00\x00\x00\x00\x00\x00\x00', datetime(1970, 1, 1, tzinfo=timezone.utc)
        # And independent of the timezone
        yield b'\x00\x00\x00\x00\x00\x00\x00\x00', datetime(1970, 1, 1, 1, 30, tzinfo=timezone(timedelta(hours=1, minutes=30)))
        # And and increase by a millisecond
        yield b'\x00\x00\x00\x00\x00\x00\x00\x01', datetime(1970, 1, 1, microsecond=1000, tzinfo=timezone.utc)
        # Cannot validate, that it is unsigned, as it is
        # yield b'\x80\x00\x00\x00\x00\x00\x00\x00', datetime(1970, 1, 1, microsecond=1000, tzinfo=timezone.utc)

    def because_we_read_a_timestamp(self, binary, _):
        self.result = serialisation.read_timestamp(BytesIO(binary))

    def it_should_read_it_correctly(self, _, expected):
        assert self.result == expected


class WhenWritingATimestamp:
    @classmethod
    def examples_of_timestamps(cls):
        for encoded, timeval in WhenParsingATimestamp.examples_of_timestamps():
            yield timeval, encoded

    def because_I_pack_them(self, timeval, _):
        self.result = serialisation.pack_timestamp(timeval)

    def it_should_pack_them_correctly(self, _, expected):
        assert self.result == expected


class WhenPackingAndUnpackingATimestamp:
    # Ensure, we do not add some offset by the serialisation process
    @classmethod
    def examples_of_timestamps(cls):
        yield datetime(1970, 1, 1, tzinfo=timezone.utc)
        yield datetime(1979, 1, 1, tzinfo=timezone(timedelta(hours=1, minutes=30)))

    def because_I_pack_them(self, timeval):
        packed = serialisation.pack_timestamp(timeval)
        unpacked = serialisation.read_timestamp(BytesIO(packed))
        self.result = unpacked - timeval

    def it_should_pack_them_correctly(self, timeval):
        assert abs(self.result.total_seconds()) < 1.0e-9
