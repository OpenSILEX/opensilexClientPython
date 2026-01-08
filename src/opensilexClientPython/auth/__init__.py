"""Authentication module for OpenSILEX.

This module provides functions to connect to OpenSILEX instances.

Example:
    >>> from opensilexClientPython.auth import connect
    >>> client = connect.connect_to_opensilex({
    ...     "host": "http://localhost:8666/rest",
    ...     "identifier": "admin@opensilex.org",
    ...     "password": "admin"
    ... })
"""
