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

    def __init__(self, channel_id, payload):
        self.channel_id = channel_id
        self.payload = payload


class HeartbeatFrame(Frame):
    frame_type = 8
    channel_id = 0
    payload = b''
