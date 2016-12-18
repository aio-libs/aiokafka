
class ConsumerStoppedError(Exception):
    """ Raised on `get*` methods of Consumer if it's cancelled, even pending
        ones.
    """
