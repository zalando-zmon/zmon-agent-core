"""
Helper functions for dealing with Kubernetes resource requests & limits
"""
import re

_UNITS = {
    'm': 0.001,
    'K': 1000,
    'M': 1000**2,
    'G': 1000**3,
    'T': 1000**4,
    'P': 1000**5,
    'E': 1000**6,
    'Ki': 1024,
    'Mi': 1024**2,
    'Gi': 1024**3,
    'Ti': 1024**4,
    'Pi': 1024**5,
    'Ei': 1024**6
}

_RESOURCE_REGEX = re.compile(r"""(?P<resource>\d+)(?P<unit>\w+)?""")


def parse_resource(resource):
    """Parse a Kubernetes resource definition into a raw float value.

    >>> parse_resource("3")
    3.0

    >>> parse_resource("30m")
    0.03

    >>> parse_resource("3K")
    3000.0

    >>> parse_resource("3M")
    3000000.0

    >>> parse_resource("3G")
    3000000000.0

    >>> parse_resource("3T")
    3000000000000.0

    >>> parse_resource("3P")
    3000000000000000.0

    >>> parse_resource("3E")
    3e+18

    >>> parse_resource("3Ki")
    3072.0

    >>> parse_resource("3Mi")
    3145728.0

    >>> parse_resource("3Gi")
    3221225472.0

    >>> parse_resource("3Ti")
    3298534883328.0

    >>> parse_resource("3Pi")
    3377699720527872.0

    >>> parse_resource("3Ei")
    3.458764513820541e+18

    >>> parse_resource("invalid")
    Traceback (most recent call last):
        ...
    ValueError: Invalid resource definition: invalid
    """
    match = re.match(_RESOURCE_REGEX, resource)
    if not match:
        raise ValueError("Invalid resource definition: " + resource)

    value = float(match.group('resource'))
    unit_name = match.group('unit')
    unit = _UNITS[unit_name] if unit_name else 1
    return value * unit
