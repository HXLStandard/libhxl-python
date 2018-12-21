""" Functions that can run inside a formula
"""

import logging, collections
import hxl.datatypes

logger = logging.getLogger(__name__)

#
# Operators (not callable as functions)
#

def ref(row, args):
    """A single tag pattern standing alone."""
    args = _deref(row, args)
    return args[0]

def add(row, args, multiple=False):
    result = 0
    for arg in _deref(row, args, multiple):
        result += _num(arg)
    return result

def subtract(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
    for arg in args[1:]:
        result -= _num(arg)
    return result

def multiply(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
    for arg in args[1:]:
        result *= _num(arg)
    return result

def divide(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
    for arg in args[1:]:
        v = _num(arg) # avoid DIV0
        if v:
            result = result / v
    return result

def modulo(row, args, multiple=False):
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
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

def function(row, name, args):
    f = functions.get(name)
    if f:
        return f(row, _deref(row, args))
    else:
        logger.error("Unknown function %s", args[0])
        return None

def sum(row, args):
    return add(row, args, True)


#
# Internal helpers
#

def _deref(row, args, multiple=False):
    """Dereference a term.
    If it's a two-element list with a function and a list, recurse.
    If it's a tag pattern, look it up in the row and replace with value(s)
    If it's already a literal (number or string), leave it alone.
    @param row: a hxl.model.Row object
    @param args: a list of arguments to dereference (may be tag patterns or literals)
    @param multiple: if true, return all matches for a tag pattern
    @return: always a list (may be empty)
    """
    result = []

    for arg in args:
        if isinstance(arg, collections.Sequence) and callable(arg[0]):
            # it's a function and args: recurse
            result.append(arg[0](row, arg[1]))
        elif isinstance(arg, hxl.model.TagPattern):
            # it's a tag pattern: look up matching values in the row
            if multiple:
                result += row.get_all(arg)
            else:
                result.append(row.get(arg))
        else:
            # it's a literal: leave it alone
            result.append(arg)

    return result

def _num(arg):
    """Convert to a number if possible.
    Otherwise, return zero and log a warning.
    """
    try:
        return hxl.datatypes.normalise_number(arg)
    except (ValueError, TypeError):
        logger.warn("Cannot convert %s to a number for calculated field", arg)
        return 0
