class Exchange(object):
    def __init__(self, name, type, durable, auto_delete, internal):
        self.name = name
        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal
