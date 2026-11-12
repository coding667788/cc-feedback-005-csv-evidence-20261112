"""Explicit exception hierarchy for the collector.

No error path is allowed to silently skip rows. Every failure raises a
specific exception that becomes a non-zero process exit code.
"""


class CollectorError(Exception):
    """Base class for all collector failures."""

    exit_code = 1


class ManifestError(CollectorError):
    """Manifest is missing, malformed or inconsistent with configuration."""

    exit_code = 2


class SourceError(CollectorError):
    """Source file fails size/hash verification or cannot be read."""

    exit_code = 3


class ParseError(CollectorError):
    """CSV content violates the structural or encoding contract.

    Covers invalid byte sequences, unclosed quoted fields and column count
    mismatches. The offending byte offset is always reported.
    """

    exit_code = 4

    def __init__(self, message, offset=None):
        if offset is not None:
            message = f"{message} (byte offset {offset})"
        super().__init__(message)
        self.offset = offset


class TamperError(CollectorError):
    """Source file changed while the collector was streaming it."""

    exit_code = 5


class AttemptsExhaustedError(CollectorError):
    """Configured retry budget for a file was exhausted."""

    exit_code = 6


class BatchError(CollectorError):
    """Batch publication or batch directory state is invalid."""

    exit_code = 7

