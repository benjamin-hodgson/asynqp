from io import BytesIO
from . import spec
from . import serialisation


def read(frame_type, channel_id, raw_payload):
    if frame_type == MethodFrame.frame_type:
        method = spec.read_method(raw_payload)
        return MethodFrame(channel_id, method)
    elif frame_type == HeartbeatFrame.frame_type:
        return HeartbeatFrame()


class Frame(object):
    def __init__(self, channel_id, payload):
        self.channel_id = channel_id
        self.payload = payload

    def serialise(self):
        frame = serialisation.pack_octet(self.frame_type)
        frame += serialisation.pack_short(self.channel_id)

        if isinstance(self.payload, bytes):
            body = self.payload
        else:
            bytesio = BytesIO()
            self.payload.write(bytesio)
            body = bytesio.getvalue()

        frame += serialisation.pack_long(len(body)) + body
        frame += serialisation.pack_octet(spec.FRAME_END)

        return frame

    def __eq__(self, other):
        return (self.frame_type == other.frame_type
                and self.channel_id == other.channel_id
                and self.payload == other.payload)


class MethodFrame(Frame):
    frame_type = spec.FRAME_METHOD


class ContentHeaderFrame(Frame):
    frame_type = spec.FRAME_HEADER


class ContentBodyFrame(Frame):
    frame_type = spec.FRAME_BODY


class HeartbeatFrame(Frame):
    frame_type = spec.FRAME_HEARTBEAT
    channel_id = 0
    payload = b''

    def __init__(self):
        pass


class ContentHeaderPayload(object):
    def __init__(self, class_id, body_length, properties):
        self.class_id = class_id
        self.body_length = body_length
        self.properties = properties

    def __eq__(self, other):
        return (self.class_id == other.class_id
                and self.body_length == other.body_length
                and self.properties == other.properties)

    def write(self, stream):
        stream.write(serialisation.pack_short(self.class_id))
        stream.write(serialisation.pack_short(0))  # weight
        stream.write(serialisation.pack_long_long(self.body_length))

        bytesio = BytesIO()

        property_flags = 0
        bitshift = 15

        for val in self.properties:
            if val is not None:
                property_flags |= (1 << bitshift)
                val.write(bytesio)
            bitshift -= 1

        stream.write(serialisation.pack_short(property_flags))
        stream.write(bytesio.getvalue())
