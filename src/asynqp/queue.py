class Queue(object):
    def __init__(self, name, durable, exclusive, auto_delete):
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
