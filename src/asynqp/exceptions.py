class AMQPError(IOError):
    pass


class UnhandledBasicReturn(ValueError):
    pass


class Deleted(ValueError):
    pass
