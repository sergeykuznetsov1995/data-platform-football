"""Runner termination must escape ordinary target and SQL failure handling."""


class FotMobTerminated(BaseException):
    """TERM requests a bounded diagnostic abort, never another payload flush."""
