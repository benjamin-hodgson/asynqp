import struct
from collections import OrderedDict
from io import BytesIO
from xml.etree import ElementTree
import pkg_resources
from . import amqptypes
from . import serialisation
from .amqptypes import FIELD_TYPES
from ._exceptions import AMQPError


def read_method(raw):
    stream = BytesIO(raw)
    method_type_code = struct.unpack('!HH', raw[0:4])
    return METHODS[method_type_code].read(stream)


class Method:
    def __init__(self, *args):
        self.fields = OrderedDict()

        if len(args) != len(self.field_info):
            raise TypeError('__init__ takes {} arguments but {} were given'.format(len(self.field_info), len(args)))

        for (fieldname, fieldcls), value in zip(self.field_info.items(), args):
            self.fields[fieldname] = fieldcls(value)

    @classmethod
    def read(cls, stream):
        method_type = struct.unpack('!HH', stream.read(4))
        assert method_type == cls.method_type, "How did this happen? Wrong method type for {}: {}".format(cls.__name__, method_type)

        args = []
        number_of_bits = 0
        for fieldcls in cls.field_info.values():
            if fieldcls is FIELD_TYPES['bit']:
                number_of_bits += 1
                continue
            elif number_of_bits:  # if we have some bools but this next field is not a bool
                val = ord(stream.read(1))
                args.extend(serialisation.read_bools(val, number_of_bits))
                number_of_bits = 0

            args.append(fieldcls.read(stream))

        if number_of_bits:  # if there were some bools right at the end
            val = ord(stream.read(1))
            args.extend(serialisation.read_bools(val, number_of_bits))
            number_of_bits = 0

        return cls(*args)

    def write(self, stream):
        stream.write(struct.pack('!HH', *self.method_type))
        bits = []
        for val in self.fields.values():
            if isinstance(val, amqptypes.Bit):
                bits.append(val.value)
            else:
                if bits:
                    stream.write(serialisation.pack_bools(*bits))
                    bits = []
                val.write(stream)

        if bits:
            stream.write(serialisation.pack_bools(*bits))

    def __getattr__(self, name):
        try:
            return self.fields[name]
        except KeyError as e:
            raise AttributeError('{} object has no attribute {}'.format(type(self).__name__, name)) from e

    def __eq__(self, other):
        return (type(self) == type(other)
                and self.fields == other.fields)


# Here, we load up the AMQP XML spec, traverse it,
# and generate serialisable DTO classes (subclasses of Method, above)
# based on the definitions in the XML file.
# It's one of those things that works like magic until it doesn't.
# Compare amqp0-9-1.xml to the classes that this module exports and you'll see what's going on
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

            doc = build_docstring(method, fields)

            synchronous = 'synchronous' in method.attrib

            method_name = method.attrib['name'].capitalize().replace('-ok', 'OK').replace('-empty', 'Empty')
            class_methods[method_name] = (int(method_id), fields, method_support, synchronous, doc)

        classes[class_elem.attrib['name'].capitalize()] = (int(class_id), class_methods)

    return classes


def build_docstring(method_elem, fields):
    doc = '\n'.join([line.strip() for line in method_elem.find('doc').text.splitlines()]).strip()
    doc += '\n\nArguments:\n    '
    doc += '\n    '.join([n + ': ' + t.__name__ for n, t in fields.items()])
    return doc


def get_constants(tree):
    constants = {}
    for elem in tree.findall('constant'):
        name = elem.attrib['name'].replace('-', '_').upper()
        value = int(elem.attrib['value'])
        constants[name] = value
    return constants


def generate_methods(classes):
    methods = {}

    for class_name, (class_id, method_infos) in classes.items():
        for method_name, (method_id, fields, method_support, synchronous, method_doc) in method_infos.items():
            name = class_name + method_name
            method_type = (class_id, method_id)

            # this call to type() is where the magic happens -
            # we are dynamically building subclasses of OutgoingMethod and/or IncomingMethod
            # with strongly-typed fields as defined in the spec.
            # The write() and read() methods of the base classes traverse the fields
            # and generate the correct bytestring
            cls = type(name, (Method,), {'method_type': method_type, 'field_info': fields, 'synchronous': synchronous})
            cls.__doc__ = method_doc
            methods[name] = methods[method_type] = cls

    return methods


def generate_exceptions(constants):
    ret = {}

    for name, value in constants.items():
        if 300 <= value < 600:  # it's an error
            classname = ''.join([x.capitalize() for x in name.split('_')])
            ret[classname] = type(classname, (AMQPError,), {})

    return ret


METHODS, CONSTANTS = load_spec()
CONSTANTS_INVERSE = {value: name for name, value in CONSTANTS.items()}
EXCEPTIONS = generate_exceptions(CONSTANTS)

# what the hack? 'response' is always a table but the protocol spec says it's a longstr.
METHODS['ConnectionStartOK'].field_info['response'] = amqptypes.Table

# Also pretty hacky
globals().update({k: v for k, v in METHODS.items() if isinstance(k, str)})
globals().update(CONSTANTS)
