""" Functions that can run inside an equation
"""

import logging
import hxl.datatypes

logger = logging.getLogger(__name__)

#
# Operators (not callable as functions)
#

def add(row, args, multiple=False):
    result = 0
    for arg in _deref(row, args, multiple):
        result += _num(arg)
    return result

def subtract(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0])
    for arg in args[1:]:
        result -= _num(arg)
    return result

def multiply(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0])
    for arg in args[1:]:
        result *= _num(arg)
    return result

def divide(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0])
    for arg in args[1:]:
        v = _num(arg) # avoid DIV0
        if v:
            result = result / v
    return result

def mod(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0])
    for arg in args[1:]:
        v = _num(arg) # avoid DIV0
        if v:
            result = result % v
    return result


#
# User-callable functions
#

functions = {
    'sum': sum
}
"""Master table of user-callable functions"""

def sum(row, args):
    return add(row, args, True)


#
# Internal helpers
#

def _deref(row, args, multiple=False):
    """Dereference a term.
    If it's already a literal (number or string), leave it alone.
    Otherwise, look it up in the row.
    @param row: a hxl.model.Row object
    @param args: a list of arguments to dereference (may be tag patterns or literals)
    @param multiple: if true, return all matches for a tag pattern
    @return: always a list (may be empty)
    """
    result = []

    for arg in args:
        if isinstance(arg, hxl.model.TagPattern):
            if multiple:
                result += row.getAll(arg)
            else:
                result.append(row.get(arg))
        else:
            result.append(arg)

    return result

def _num(arg):
    """Convert to a number if possible.
    Otherwise, return zero and log a warning.
    """
    try:
        return hxl.datatypes.normalise_number(arg)
    except ValueError:
        logger.warn("Cannot convert %s to a number for calculated field", arg)
        return 0
