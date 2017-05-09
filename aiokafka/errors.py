
class ConsumerStoppedError(Exception):
    """ Raised on `get*` methods of Consumer if it's cancelled, even pending
        ones.
    """


class IllegalOperation(Exception):
    """ Raised if you try to execute an operation, that is not available with
        current configuration. For example trying to commit if no group_id was
        given.
    """
