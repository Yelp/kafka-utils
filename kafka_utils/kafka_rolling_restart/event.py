class Event(object):
    """Class to be used to start/perform events before and after
    rolling restart"""

    def __init__(self, opts):
        self.opts = opts

    def start(self):
        raise NotImplementedError("implemented in subclass")

    def stop(self):
        raise NotImplementedError("implemneted in subclass")
