import warnings

DEPRECATION_WARNING = (
    "API for this function change. Please use it as coroutine. "
    "Function usage will stop working in next minor release."
)


class FunctionToCoroDeprecation:

    def __init__(self, coro, warning):
        self._coro = coro
        self._yielded = False
        self._warning = warning

    def __await__(self):
        self._yielded = True
        return self._coro.__await__()

    def __iter__(self):
        self._yielded = True
        return self._coro.__iter__()

    def __repr__(self):
        return repr(self._coro)

    # Should be ok for all python versions. This function should only operate
    # on ref counting, so should not be GC'ed
    def __del__(self, _warnings=warnings):
        if not self._yielded:
            _warnings.warn(self._warning, DeprecationWarning)
