"""Validation exception types."""


class ValidationCommandError(ValueError):
    """Raised when CLI arguments or invocation state are invalid."""


class ValidationExecutionError(RuntimeError):
    """Raised when validation cannot be executed due to repository state."""
