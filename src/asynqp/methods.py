import abc
import struct
from collections import OrderedDict
from io import BytesIO
from xml.etree import ElementTree
import pkg_resources
from . import serialisation


MAX_OCTET = 0xFF
MAX_SHORT = 0xFFFF
MAX_LONG = 0xFFFFFFFF
MAX_LONG_LONG = 0xFFFFFFFFFFFFFFFF


def deserialise_method(raw_payload):
    method_type_code = struct.unpack('!HH', raw_payload[0:4])
    return METHODS[method_type_code].deserialise(raw_payload)


# what a hack! The application wants 'response' to be a table
# but the protocol requires it to be a longstr.
def make_connection_start_ok(client_properties, mechanism, response, locale):
    meth = METHODS['ConnectionStartOK'](client_properties, mechanism, '', locale)
    meth.fields['response'] = Table(response)
    return meth


class FieldType(abc.ABC):
    def __init__(self, value):
        if not self.isvalid(value):
            raise TypeError('{} is not a valid value for type {}'.format(value, type(self).__name__))
        self.value = value

    def __eq__(self, other):
        if isinstance(other, type(self.value)):
            return self.value == other
        try:
            return self.value == other.value
        except AttributeError:
            return NotImplemented

    @abc.abstractmethod
    def isvalid(self, value):
        pass

    @abc.abstractmethod
    def serialise(self):
        pass

    @classmethod
    @abc.abstractmethod
    def read(cls, stream):
        pass


class Bit(FieldType):
    def isvalid(self, value):
        return isinstance(value, bool)

    def serialise(self):
        return serialisation.pack_bool(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_bool(stream))


class Octet(FieldType):
    def isvalid(self, value):
        return isinstance(value, int) and 0 <= value <= MAX_OCTET

    def serialise(self):
        return serialisation.pack_octet(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_octet(stream))


class Short(FieldType):
    def isvalid(self, value):
        return isinstance(value, int) and 0 <= value <= MAX_SHORT

    def serialise(self):
        return serialisation.pack_short(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short(stream))


class Long(FieldType):
    def isvalid(self, value):
        return isinstance(value, int) and 0 <= value <= MAX_LONG

    def serialise(self):
        return serialisation.pack_long(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long(stream))


class LongLong(FieldType):
    def isvalid(self, value):
        return isinstance(value, int) and 0 <= value <= MAX_LONG_LONG

    def serialise(self):
        return serialisation.pack_long_long(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_long(stream))


class ShortStr(FieldType):
    def isvalid(self, value):
        return isinstance(value, str) and len(value) <= MAX_OCTET

    def serialise(self):
        return serialisation.pack_short_string(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short_string(stream))


class LongStr(FieldType):
    def isvalid(self, value):
        return isinstance(value, str) and len(value) <= MAX_LONG

    def serialise(self):
        return serialisation.pack_long_string(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_string(stream))


class Table(FieldType):
    def isvalid(self, value):
        return isinstance(value, dict)

    def serialise(self):
        return serialisation.pack_table(self.value)

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_table(stream))


FIELD_TYPES = {
    'bit': Bit,
    'octet': Octet,
    'short': Short,
    'long': Long,
    'longlong': LongLong,
    'table': Table,
    'longstr': LongStr,
    'shortstr': ShortStr
}


class Method:
    field_info = []  # list of (name, class) tuples
    def __init__(self, *args):
        self.fields = OrderedDict()

        if len(args) != len(self.field_info):
            raise TypeError('__init__ takes {} arguments but {} were given'.format(len(self.field_info), len(args)))

        for (fieldname, fieldcls), value in zip(self.field_info, args):
            self.fields[fieldname] = fieldcls(value)

    def __getattr__(self, name):
        try:
            return self.fields[name]
        except KeyError as e:
            raise AttributeError('{} object has no attribute {}'.format(type(self).__name__, name)) from e

    def __eq__(self, other):
        return (type(self) == type(other)
            and self.fields == other.fields)

    def serialise(self):
        ret = struct.pack('!HH', *self.method_type)
        for val in self.fields.values():
            ret += val.serialise()
        return ret

    @classmethod
    def deserialise(cls, raw):
        stream = BytesIO(raw)
        method_type = struct.unpack('!HH', stream.read(4))
        if method_type != cls.method_type:
            raise ValueError("How did this happen? Wrong method type for {}: {}".format(cls.__name__, method_type))

        args = []
        for fieldname, fieldcls in cls.field_info:
            args.append(fieldcls.read(stream).value)

        return cls(*args)


def make_method_subclass(name, method_type, fields):
    return type(name, (Method,), {'method_type': method_type, 'field_info': fields})


# here be monsters
def load_methods():
    filename = pkg_resources.resource_filename(__name__, 'amqp0-9-1.xml')
    tree = ElementTree.parse(filename)
    domain_types = {e.attrib['name']: e.attrib['type'] for e in tree.findall('domain')}

    classes = {}
    for class_elem in tree.findall('class'):
        class_id = class_elem.attrib['index']
        class_methods = {}
        for method in class_elem.findall('method'):
            method_id = method.attrib['index']
            fields = []
            for elem in method.findall('field'):
                fieldname = elem.attrib['name'].replace('-','_')
                try:
                    fieldtype = elem.attrib['type']
                except KeyError:
                    fieldtype = domain_types[elem.attrib['domain']]
                cls = FIELD_TYPES[fieldtype]
                fields.append((fieldname, cls))
            class_methods[method.attrib['name'].capitalize().replace('-ok', 'OK')] = (int(method_id), fields)

        classes[class_elem.attrib['name'].capitalize()] = (int(class_id), class_methods)

    methods = {}

    for class_name, (class_id, ms) in classes.items():
        for method_name, (method_id, fields) in ms.items():
            name = class_name + method_name
            method_type = (class_id, method_id)
            methods[name] = methods[method_type] = make_method_subclass(name, method_type, fields)

    return methods


METHODS = load_methods()
