import abc
import functools
import operator
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


def read_method(raw):
    stream = BytesIO(raw)
    method_type_code = struct.unpack('!HH', raw[0:4])
    return METHODS[method_type_code].read(stream)


class FieldType(abc.ABC):
    def __init__(self, value):
        if isinstance(value, type(self)):
            value = value.value  # ha!
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

    def __repr__(self):
        return "{}({})".format(type(self).__name__, self.value)

    @classmethod
    @abc.abstractmethod
    def isvalid(cls, value):
        pass

    @abc.abstractmethod
    def write(self, stream):
        pass

    @classmethod
    @abc.abstractmethod
    def read(cls, stream):
        pass

    def operate(self, func, other):
        if isinstance(other, type(self.value)):
            return type(self)(func(self.value, other))
        try:
            return type(self)(func(self.value, other.value))
        except (AttributeError, TypeError):
            return NotImplemented


@functools.total_ordering
class OrderedFieldType(FieldType):
    __lt__ = functools.partialmethod(FieldType.operate, operator.lt)


class NumericFieldType(OrderedFieldType):
    __add__ = functools.partialmethod(FieldType.operate, operator.add)
    __sub__ = functools.partialmethod(FieldType.operate, operator.sub)
    __mul__ = functools.partialmethod(FieldType.operate, operator.mul)
    __truediv__ = functools.partialmethod(FieldType.operate, operator.truediv)
    __floordiv__ = functools.partialmethod(FieldType.operate, operator.floordiv)


class IntegralFieldType(NumericFieldType):
    def __int__(self):
        return int(self.value)

    def __index__(self):
        return operator.index(self.value)


class StringFieldType(FieldType):
    def __str__(self):
        return str(self.value)


class Bit(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, bool)

    def write(self, stream):
        stream.write(serialisation.pack_bool(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_bool(stream))


class Octet(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_OCTET

    def write(self, stream):
        stream.write(serialisation.pack_octet(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_octet(stream))


class Short(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_SHORT

    def write(self, stream):
        stream.write(serialisation.pack_short(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short(stream))


class Long(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_LONG

    def write(self, stream):
        stream.write(serialisation.pack_long(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long(stream))


class LongLong(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_LONG_LONG

    def write(self, stream):
        stream.write(serialisation.pack_long_long(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_long(stream))


class ShortStr(FieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, str) and len(value) <= MAX_OCTET

    def write(self, stream):
        stream.write(serialisation.pack_short_string(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short_string(stream))


class LongStr(FieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, str) and len(value) <= MAX_LONG

    def write(self, stream):
        stream.write(serialisation.pack_long_string(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_string(stream))


class Table(FieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, dict)

    def write(self, stream):
        stream.write(serialisation.pack_table(self.value))

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
    def __init__(self, *args):
        self.fields = OrderedDict()

        if len(args) != len(self.field_info):
            raise TypeError('__init__ takes {} arguments but {} were given'.format(len(self.field_info), len(args)))

        for (fieldname, fieldcls), value in zip(self.field_info.items(), args):
            self.fields[fieldname] = fieldcls(value)

    def __getattr__(self, name):
        try:
            return self.fields[name]
        except KeyError as e:
            raise AttributeError('{} object has no attribute {}'.format(type(self).__name__, name)) from e

    def __eq__(self, other):
        return (type(self) == type(other)
                and self.fields == other.fields)


class OutgoingMethod(Method):
    def write(self, stream):
        stream.write(struct.pack('!HH', *self.method_type))
        for val in self.fields.values():
            val.write(stream)


class IncomingMethod(Method):
    @classmethod
    def read(cls, stream):
        method_type = struct.unpack('!HH', stream.read(4))
        if method_type != cls.method_type:
            raise ValueError("How did this happen? Wrong method type for {}: {}".format(cls.__name__, method_type))

        args = []
        for fieldname, fieldcls in cls.field_info.items():
            args.append(fieldcls.read(stream).value)

        return cls(*args)


# here be monsters
def load_spec():
    tree = parse_tree()
    classes = get_classes(tree)
    return generate_methods(classes), get_constants(tree)


def parse_tree():
    filename = pkg_resources.resource_filename(__name__, 'amqp0-9-1.xml')
    return ElementTree.parse(filename)


def get_classes(tree):
    domain_types = {e.attrib['name']: e.attrib['type'] for e in tree.findall('domain')}

    classes = {}
    for class_elem in tree.findall('class'):
        class_id = class_elem.attrib['index']

        class_methods = {}
        for method in class_elem.findall('method'):
            method_id = method.attrib['index']

            fields = OrderedDict()
            for elem in method.findall('field'):
                fieldname = elem.attrib['name'].replace('-', '_')
                try:
                    fieldtype = elem.attrib['type']
                except KeyError:
                    fieldtype = domain_types[elem.attrib['domain']]
                cls = FIELD_TYPES[fieldtype]
                fields[fieldname] = cls

            method_support = {}
            for elem in method.findall('chassis'):
                method_support[elem.attrib['name']] = elem.attrib['implement']

            class_methods[method.attrib['name'].capitalize().replace('-ok', 'OK')] = (int(method_id), fields, method_support)

        classes[class_elem.attrib['name'].capitalize()] = (int(class_id), class_methods)

    return classes


def get_constants(tree):
    constants = {}
    for elem in tree.findall('constant'):
        name = elem.attrib['name'].replace('-', '_').upper()
        value = int(elem.attrib['value'])
        constants[name] = value
    return constants


def generate_methods(classes):
    methods = {}
    for class_name, (class_id, ms) in classes.items():
        for method_name, (method_id, fields, method_support) in ms.items():
            name = class_name + method_name
            method_type = (class_id, method_id)

            parents = []
            if 'server' in method_support:
                parents.append(OutgoingMethod)
            if 'client' in method_support:
                parents.append(IncomingMethod)

            methods[name] = methods[method_type] = type(name, tuple(parents), {'method_type': method_type, 'field_info': fields})
    return methods


METHODS, CONSTANTS = load_spec()

# what a hack! 'response' is almost always a table but the protocol spec says it's a longstr.
METHODS['ConnectionStartOK'].field_info['response'] = Table

# Also pretty hacky
globals().update(METHODS)
globals().update(CONSTANTS)
