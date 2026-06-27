# sdk/pulsedb/exceptions.py

class PulseDBError(Exception):
    """Base exception for all PulseDB SDK errors."""


class ConnectionError(PulseDBError):
    """Could not connect to the PulseDB server."""


class AuthenticationError(PulseDBError):
    """API key was rejected."""


class CommandError(PulseDBError):
    """The server returned an ERROR response."""


class TimeoutError(PulseDBError):
    """A command or connection timed out."""
