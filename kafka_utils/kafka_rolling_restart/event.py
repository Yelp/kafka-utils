class Event(object):

    def __init__(self, opts):
        self.opts = opts

    def start(self):
        raise NotImplementedError("implemented in subclass")

    def stop(self):
        raise NotImplementedError("implemneted in subclass")
