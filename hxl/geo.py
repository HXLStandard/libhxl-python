"""
Geodata operations
David Megginson
Started February 2018

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

import re

# regular expression fragments
DEG_RE = '(?P<deg>\d+(?:\.\d*)?)\s*\°?'
MIN_RE = '(?P<min>\d+(?:\.\d*)?)\s*[\'`′]?'
SEC_RE = '(?P<sec>\d+(?:\.\d*)?)\s*(?:["“”″]|[\'`′][\'`′])?'

LAT_PATTERNS = (
    re.compile(
        '^(?P<sign>[+-])?\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # -00 00 00
    re.compile(
        '^(?P<hemi>[NS])\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # S 00 00 00
    re.compile(
        '^{}\s*(?P<hemi>[NS])\s*(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # 00 S 00 00
    re.compile(
        '^{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)\s*(?P<hemi>[NS])?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # 00 00 00 S
)
"""Regular expressions for parsing latitude strings"""

LON_PATTERNS = (
    re.compile(
        '^(?P<sign>[+-])?\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # -00 00 00
    re.compile(
        '^(?P<hemi>[EW])\s*{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # S 00 00 00
    re.compile(
        '^{}\s*(?P<hemi>[EW])\s*(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # 00 S 00 00
    re.compile(
        '^{}(?:[\s:;,-]*{}(?:[\s:;,-]*{})?)\s*(?P<hemi>[EW])?$'.format(
            DEG_RE, MIN_RE, SEC_RE
        ), flags=re.I
    ), # 00 00 00 S
)
"""Regular expressions for parsing longitude strings"""

def _make_num(parts, max_deg):
    """Parse the parts from a regular expression match into a decimal number.
    @param parts: the parts from the regex match
    @param max_deg: the maximum degrees allowed (90 or 180)
    @returns: a floating point representation
    @exception: if part of the latitude or longitude is out of range
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

def parse_lat(s):
    """Parse a latitude string
    @param s: an input string to parse
    @returns: decimal longitude, or None on failure
    @exception ValueError: if part of the latitud is out of allowed range
    """
    s = s.strip()
    for pattern in LAT_PATTERNS:
        result = re.match(pattern, s)
        if result:
            try:
                lat = _make_num(result.groupdict(), max_deg=90)
            except ValueError as e:
                raise ValueError('failed to parse latitude {}: {}'.format(s, e.args[0]))
            return lat
    return None

def parse_lon(s):
    """Parse a longitude string
    @param s: an input string to parse
    @returns: decimal longitude, or None on failure
    @exception ValueError: if part of the longitude is out of allowed range
    """
    s = s.strip()
    for pattern in LON_PATTERNS:
        result = re.match(pattern, s)
        if result:
            try:
                lat = _make_num(result.groupdict(), max_deg=180)
            except ValueError as e:
                raise ValueError('failed to parse latitude {}: {}'.format(s, e.args[0]))
            return lat
    return None

def parse_coord(s):
    """Parse lat/lon separated by a delimiter [/,:; ]
    @param s: an input string to parse
    @returns: a tuple with decimal latitude and longitude, or None on failure
    @exception ValueError: if part of the latitude or longitude is out of allowed range
    """
    for delim in ('/', ',', ':', ';', ' ',):
        if s.find(delim) > 0:
            parts = s.split(delim)
            if len(parts) == 2:
                lat = parse_lat(parts[0])
                lon = parse_lon(parts[1])
                if lat and lon:
                    return (lat, lon,)
    return None
