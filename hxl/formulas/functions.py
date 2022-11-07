"""Functions that can run inside a formula
"""

import logging, collections
import hxl.datatypes
import datetime

from hxl.util import logup

logger = logging.getLogger(__name__)

#
# Operators (not directly callable as functions, but see below)
#

def const(row, args, multiple=False):
    """A constant value (returns itself).
    """
    return args[0]

def tagref(row, args):
    """A single tag pattern standing alone.
    @param row: the HXL data row
    @param args: the arguments parsed
    """
    return row.get(args[0])

def add(row, args, multiple=False):
    """An addition statement
    X + Y
    @param row: the HXL data row
    @param args: the arguments parsed
    @param multiple: if true, allow tag patterns to expand to multiple values (used only for function form, not operator form)
    @returns: the sum of the arguments
    """
    result = 0
    for arg in _deref(row, args, multiple):
        result += _num(arg)
    return result

def subtract(row, args, multiple=False):
    """A subtraction statement
    X - Y
    @param row: the HXL data row
    @param args: the arguments parsed
    @param multiple: if true, allow tag patterns to expand to multiple values (used only for function form, not operator form)
    @returns: the result of subtracting all of the following arguments from the first one
    """
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
    for arg in args[1:]:
        result -= _num(arg)
    return result

def multiply(row, args, multiple=False):
    """A multiplication statement
    X * Y
    @param row: the HXL data row
    @param args: the arguments parsed
    @param multiple: if true, allow tag patterns to expand to multiple values (used only for function form, not operator form)
    @returns: the product of the arguments
    """
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
    for arg in args[1:]:
        result *= _num(arg)
    return result

def divide(row, args, multiple=False):
    """A division statement
    X / Y
    @param row: the HXL data row
    @param args: the arguments parsed
    @param multiple: if true, allow tag patterns to expand to multiple values (used only for function form, not operator form)
    @returns: the result of dividing the first argument by all of the following ones, in order.
    """
    args = _deref(row, args, multiple)
    result = _num(args[0]) if len(args) > 0 else 0
    for arg in args[1:]:
        v = _num(arg) # avoid DIV0
        if v:
            result = result / v
        else:
            return 'NaN'
    return result

def modulo(row, args, multiple=False):
    """A modulo division statement
    X / Y
    @param row: the HXL data row
    @param args: the arguments parsed
    @param multiple: if true, allow tag patterns to expand to multiple values (used only for function form, not operator form)
    @returns: the remainder from dividing the first argument by all of the following ones, in order.
    """
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

def function(row, args):
    """Execute a named function
    function(arg, arg...)
    @param row: the HXL data row
    @param args: the arguments parsed (the first one is the function name)
    @returns: the result of executing the function on the arguments
    """
    f = FUNCTIONS.get(args[0])
    if f:
        return f(row, args[1:], True)
    else:
        logup('Unknown function', {"function": args[0]}, level='error')
        logger.error("Unknown function %s", args[0])
        return ''

def do_min(row, args, multiple=True):
    """Find the minimum value in the list.
    If they're all numbers (or empty), use numeric comparison.
    Otherwise, use lexical comparison (case- and space-insensitive)
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the minimum value
    """

    values = _deref(row, args, multiple)

    # first, try a numbery comparison
    try:
        min_value = None
        for value in values:
            if not hxl.datatypes.is_empty(value):
                value = hxl.datatypes.normalise_number(value)
                if min_value is None or min_value > value:
                    min_value = value
        return min_value
    # if that fails, revert to lexical
    except:
        min_value = None
        min_value_norm = None
        for value in values:
            if not hxl.datatypes.is_empty(value):
                norm = hxl.datatypes.normalise_string(value)
                if min_value_norm is None or norm < min_value_norm:
                    min_value_norm = norm
                    min_value = value
        return min_value

def do_max(row, args, multiple=True):
    """Find the maximum value in the list.
    If they're all numbers (or empty), use numeric comparison.
    Otherwise, use lexical comparison (case- and space-insensitive)
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the maximum value
    """

    values = _deref(row, args, multiple)

    # first, try a numbery comparison
    try:
        max_value = None
        for value in values:
            if not hxl.datatypes.is_empty(value):
                value = hxl.datatypes.normalise_number(value)
                if max_value is None or max_value < value:
                    max_value = value
        return max_value
    # if that fails, revert to lexical
    except:
        max_value = None
        max_value_norm = None
        for value in values:
            if not hxl.datatypes.is_empty(value):
                norm = hxl.datatypes.normalise_string(value)
                if max_value_norm is None or norm > max_value_norm:
                    max_value_norm = norm
                    max_value = value
        return max_value

def do_average(row, args, multiple=True):
    """Calculate the average (mean) of the arguments
    Ignores any cell that does not contain a number.
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the mean of all numeric arguments, or empty string if none found
    """
    values = _deref(row, args, multiple)

    total = 0
    count = 0

    # look for numbers
    for value in values:
        try:
            total += hxl.datatypes.normalise_number(value)
            count += 1
        except:
            pass # not a number

    # if there were no numbers, don't return a result
    if count > 0:
        return total / count
    else:
        return ''

def do_round(row, args, multiple=False):
    """Round a single value to the nearest integer.
    @param row: the HXL data row
    @param args: the function argument (name removed from start)
    @returns: the first argument, rounded if it's a number, or unchanged otherwise
    """
    values = _deref(row, args, False)
    if len(values) > 1:
        logup('Ignoring extra arguments to round()', {"args": str(values[1:])}, level='warning')
        logger.warning("Ignoring extra arguments to round(): %s", str(values[1:]))
    try:
        return round(values[0])
    except:
        logup('Trying to round non-numeric value', {"value": str(values[0])}, level='warning')
        logger.warning("Trying to round non-numeric value %s", values[0])
        return values[0]

def do_join(row, args, multiple=True):
    """Join values with the separator provided.
    Also joins empty values (for consistency)
    USAGE: join(sep, value1[, ...])
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: all of the arguments, joined together
    """
    values = _deref(row, args, multiple)
    separator = values[0]
    return separator.join(values[1:])


def do_today(row, args, multiple=False):
    """Return the current date (UTC) in ISO format YYYY-mm-dd
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the current UTC date in ISO YYYY-mm-dd format
    """
    return datetime.datetime.utcnow().strftime('%Y-%m-%d')


def do_datedif(row, args, multiple=False):
    """Calculate the difference between the first date and the second.
    The optional internal units arg determines the unit of measurement.
    USAGE: datedif(date1, date2[, unit])
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the difference between the dates as an integer.
    """
    values = _deref(row, args, multiple)
    if len(values) == 2:
        unit = 'D'
    elif len(values) == 3:
        unit = str(values[2]).upper()
    else:
        logup('Wrong number of arguments to datedif()', level='error')
        logger.error("Wrong number of arguments to datedif()")
        return ''
    try:
        date1 = datetime.datetime.strptime(hxl.datatypes.normalise_date(values[0]), '%Y-%m-%d')
    except:
        logup("Can't parse date", {"date": str(values[0])}, level='error')
        logger.error("Can't parse date: %s", values[0])
        return ''
    try:
        date2 = datetime.datetime.strptime(hxl.datatypes.normalise_date(values[1]), '%Y-%m-%d')
    except:
        logup("Can't parse date", {"date": str(values[1])}, level='error')
        logger.error("Can't parse date: %s", values[1])
        return ''
    diff = date2-date1
    if unit == 'Y':
        return int(abs(diff.days/365))
    elif unit == 'M':
        return abs(int(round(diff.days/30)))
    elif unit == 'W':
        return abs(int(round(diff.days/7)))
    elif unit == 'D':
        return abs(diff.days)
    else:
        logup('Unrecognised unit for datediff()', {"unit": str(unit)}, level='error')
        logger.error("Unrecognised unit %s for datediff()", str(unit))
        return ''


def do_toupper(row, args, multiple=False):
    """Convert the value to a string in upper case
    USAGE: toupper(value)
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the value as an upper-case string
    """
    values = _deref(row, args, multiple)
    return str(values[0]).upper()


def do_tolower(row, args, multiple=False):
    """Convert the value to a string in lower case
    USAGE: tolower(value)
    @param row: the HXL data row
    @param args: the function arguments (name removed from start)
    @returns: the value as an upper-case string
    """
    values = _deref(row, args, multiple)
    return str(values[0]).lower()


FUNCTIONS = {
    'sum': lambda row, args, multiple: add(row, args, multiple),
    'product': lambda row, args, multiple: multiply(row, args, multiple),
    'min': do_min,
    'max': do_max,
    'average': do_average,
    'round': do_round,
    'join': do_join,
    'today': do_today,
    'datedif': do_datedif,
    'toupper': do_toupper,
    'tolower': do_tolower,
}
"""Master table of user-callable functions"""


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
        if isinstance(arg, collections.abc.Sequence) and callable(arg[0]):
            # it's a function and args: recurse
            if arg[0] == tagref:
                result += _deref(row, arg[1], multiple)
            else:
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
    if not arg:
        return 0
    try:
        return hxl.datatypes.normalise_number(arg)
    except (ValueError, TypeError):
        logup('Cannot convert to a number for calculated field', {"arg": arg}, level='warning')
        logger.warning("Cannot convert %s to a number for calculated field", arg)
        return 0
