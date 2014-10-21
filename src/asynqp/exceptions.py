class AMQPError(IOError):
    pass


class UnhandledBasicReturn(ValueError):
    def __init__(self, reply_code, message, exchange_name, routing_key):
        self.reply_code = reply_code
        self.message = message
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        super().__init__(reply_code, message, exchange_name, routing_key)


class Deleted(ValueError):
    pass
