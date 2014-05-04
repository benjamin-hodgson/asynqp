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
        self.sender.send_content(message)
