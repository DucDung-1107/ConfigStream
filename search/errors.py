class SearchTimeoutError(TimeoutError):
    """The backend returned an incomplete response after its query deadline."""


class IncompleteSearchError(RuntimeError):
    """The backend returned results from only part of the requested search."""
