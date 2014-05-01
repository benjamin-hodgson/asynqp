import struct
from collections import OrderedDict
from io import BytesIO
from xml.etree import ElementTree
import pkg_resources
from . import amqptypes
from .amqptypes import FIELD_TYPES


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

# what the hack? 'response' is almost always a table but the protocol spec says it's a longstr.
METHODS['ConnectionStartOK'].field_info['response'] = amqptypes.Table

# Also pretty hacky
globals().update(METHODS)
globals().update(CONSTANTS)
