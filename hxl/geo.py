"""
Geodata operations

Functions for parsing latitudes and longitudes

May add more geo ops here later (e.g. filtering by boundaries).

Author:
    David Megginson

License:
    Public Domain

"""

import hxl, logging, re

__all__ = ["LAT_PATTERNS", "LON_PATTERNS", "parse_lat", "parse_lon", "parse_coord"]

logger = logging.getLogger(__name__)



########################################################################
# Constants
########################################################################

# regular expression fragments
_DEG_RE = r'(?P<deg>\d+(?:\.\d*)?)\s*\°?'
_MIN_RE = r'(?P<min>\d+(?:\.\d*)?)\s*[\'`′]?'
_SEC_RE = r'(?P<sec>\d+(?:\.\d*)?)\s*(?:["“”″]|[\'`′][\'`′])?'

LAT_PATTERNS = (
    re.compile(
        r'^(?P<sign>[+-])?\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # -00 00 00
    re.compile(
        r'^(?P<hemi>[NS])\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # S 00 00 00
    re.compile(
        r'^{}\s*(?P<hemi>[NS])\s*(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # 00 S 00 00
    re.compile(
        r'^{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)\s*(?P<hemi>[NS])?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # 00 00 00 S
)
"""List of regular expressions for parsing latitude strings"""


LON_PATTERNS = (
    re.compile(
        r'^(?P<sign>[+-])?\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # -00 00 00
    re.compile(
        r'^(?P<hemi>[EW])\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # S 00 00 00
    re.compile(
        r'^{}\s*(?P<hemi>[EW])\s*(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # 00 S 00 00
    re.compile(
        r'^{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)\s*(?P<hemi>[EW])?$'.format(
            _DEG_RE, _MIN_RE, _SEC_RE
        ), flags=re.I
    ), # 00 00 00 S
)
"""List of regular expressions for parsing longitude strings"""



########################################################################
# Functions
########################################################################

def _make_degrees_digital(parts, max_deg):
    """Assemble the degrees, minutes, and seconds parts from a regular expression result into a decimal number.
    """
    num = float(parts['deg'])
    if num > max_deg or num < max_deg*-1:
        raise ValueError('degrees out of range {}/{}'.format(max_deg*-1, max_deg))
    if parts['min']:
        min = float(parts['min'])
        if min >= 60.0:
            raise ValueError('minutes must be less than 60')
        else:
            num += min/60.0
    if parts['sec']:
        sec = float(parts['sec'])
        if sec >= 60:
            raise ValueError('seconds must be less than 60')
        num += sec/3600.0
    if parts.get('sign') == '-' or (parts.get('hemi') and parts['hemi'].upper() in ('S', 'W')):
        num *= -1
    return num


def parse_lat(value):
    """Parse a latitude string

    Accepts a wide range of formats, as defined in LAT_PATTERNS

    Examples:
        ```
        lat = parse_lat("45.5000000") # => 45.5
        lat = parse_lat("45:30N") # => 45.5
        ```

    Args:
        value (str): the input string to parse

    Returns:
        float: decimal degrees latitude, or None if it can't be parsed.

    Raises:
        ValueError: if the input is out of allowed range
    
    """
    value = hxl.datatypes.normalise_space(value)
    for pattern in LAT_PATTERNS:
        result = re.match(pattern, value)
        if result:
            try:
                lat = _make_degrees_digital(result.groupdict(), max_deg=90)
            except ValueError as e:
                raise ValueError('failed to parse latitude {}: {}'.format(value, e.args[0]))
            return lat
    return None


def parse_lon(value):
    """Parse a longitude string

    Accepts a wide range of formats, as defined in LON_PATTERNS

    Examples:
        ```
        lon = parse_lon("-75.5000000") # => -75.5
        lon = parse_lon("75:30W") # => -75.5
        ```

    Args:
        value (str): the input string to parse

    Returns:
        float: decimal degrees longitude, or None if it can't be parsed.

    Raises:
        ValueError: if the input is out of allowed range
    
    """
    value = hxl.datatypes.normalise_space(value)
    for pattern in LON_PATTERNS:
        result = re.match(pattern, value)
        if result:
            try:
                lat = _make_degrees_digital(result.groupdict(), max_deg=180)
            except ValueError as e:
                raise ValueError('failed to parse latitude {}: {}'.format(value, e.args[0]))
            return lat
    return None


def parse_coord(value):
    """Parse lat/lon separated by a delimiter [/,:; ]

    Examples:
        ```
        coord = parse_coord("45.500000;-75.5000000") # => (45.5, -75.5,)
        coord = parse_coord("45:30N, 75:30W") # => (45.5, -75.5,)
        ```

    Args:
        value (str): the lat/lon coordinate string to parse

    Returns:
        tuple: the latitude and longitude as float values, or None if unparseable

    Raises:
        ValueError: if either of the coordinates is out of range
    """
    for delim in ('/', ',', ':', ';', ' ',):
        if value.find(delim) > 0:
            parts = value.split(delim)
            if len(parts) == 2:
                lat = parse_lat(parts[0])
                lon = parse_lon(parts[1])
                if lat and lon:
                    return (lat, lon,)
    return None
