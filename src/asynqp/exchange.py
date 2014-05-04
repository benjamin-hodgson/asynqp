from . import frames
from . import spec


class Exchange(object):
    def __init__(self, sender, name, type, durable, auto_delete, internal):
        self.sender = sender
        self.name = name
        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal

    def publish(self, message, routing_key, *, mandatory=True):
        self.sender.send_BasicPublish(self.name, routing_key, mandatory, False)

        header_payload = message.header_payload(spec.BasicPublish.method_type[0])
        header_frame = frames.ContentHeaderFrame(self.sender.channel_id, header_payload)
        self.sender.protocol.send_frame(header_frame)

        for payload in message.frame_payloads(100):
            frame = frames.ContentBodyFrame(self.sender.channel_id, payload)
            self.sender.protocol.send_frame(frame)
