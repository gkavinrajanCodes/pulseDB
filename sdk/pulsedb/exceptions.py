# Copyright (c) 2026 G Kavinrajan. All rights reserved.
# Licensed under the Business Source License 1.1

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
