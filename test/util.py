def any(cls):
    class ANY(cls):
        def __init__(self):
            pass

        def __eq__(self, other):
            return isinstance(other, cls)
    return ANY()
