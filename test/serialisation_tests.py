from io import BytesIO
import contexts
import datetime
from asynqp import serialisation, AMQPError


class WhenParsingATable:
    @classmethod
    def examples_of_tables(self):
        yield b"\x00\x00\x00\x0E\x04key1t\x00\x04key2t\x01", {'key1': False, 'key2': True}
        yield b"\x00\x00\x00\x0B\x03keys\x05hello", {'key': 'hello'}
        yield b"\x00\x00\x00\x0E\x03keyS\x00\x00\x00\x05hello", {'key': 'hello'}
        yield b"\x00\x00\x00\x16\x03keyF\x00\x00\x00\x0D\x0Aanotherkeyt\x00", {'key': {'anotherkey': False}}

    def because_we_read_the_table(self, bytes, expected):
        self.result = serialisation.read_table(BytesIO(bytes))

    def it_should_return_the_table(self, bytes, expected):
        assert self.result == expected


class WhenPackingAndUnpackingATable:
    @classmethod
    def examples_of_tables(cls):
        for encoded, table in WhenParsingATable.examples_of_tables():
            yield table
        yield {'a': (1 << 16), 'b': (1 << 15)}
        yield {'c': 65535, 'd': -65535}
        yield {'e': -65536}
        yield {'f': -(1 << 63), 'g': ((1 << 64) - 1)}
        yield {'f': (1 << 32), 'g': (1 << 63)}
        yield {'t': datetime.datetime(2038, 1, 1, 3, 14, 9, 123000) }

    def because_we_pack_and_unpack_the_table(self, table):
        self.result = serialisation.read_table(BytesIO(serialisation.pack_table(table)))

    def it_should_return_the_table(self, table):
        assert self.result == table

class WhenParsingABadTable:
    @classmethod
    def examples_of_bad_tables(self):
        yield b"\x00\x00\x00\x0F\x04key1t\x00\x04key2t\x01"  # length too long
        yield b"\x00\x00\x00\x06\x04key1X"  # bad value type code

    def because_we_read_the_table(self, bytes):
        self.exception = contexts.catch(serialisation.read_table, BytesIO(bytes))

    def it_should_throw_an_AMQPError(self):
        assert isinstance(self.exception, AMQPError)


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

class WhenParsingATimeStamp:
    @classmethod
    def examples_of_time_stamps(cls):
        yield b'\x00\x00\x01\xf3\xa3\x16\x9d\xe3', datetime.datetime(2038, 1, 1, 3, 14, 9, 123000)

    def because_we_read_a_time_stamp(self, binary, _):
        self.result = serialisation.read_time_stamp(BytesIO(binary))

    def it_should_reat_it_correctly(self, _, expected):
        assert self.result == expected

class WhenWritingATimeStamp:
    @classmethod
    def examples_of_time_stamps(cls):
        for encoded, timeval in WhenParsingATimeStamp.examples_of_time_stamps():
            yield timeval, encoded

    def because_I_pack_them(self, timeval, _):
        print(repr(timeval))
        self.result = serialisation.pack_time_stamp(timeval)

    def it_should_pack_them_correctly(self, _, expected):
        assert self.result == expected
