""" Evaluate a formula against a row
"""

import logging
import hxl.formulas.parser as p, hxl.formulas.lexer as l

from hxl.util import logup

logger = logging.getLogger(__name__)

def eval(row, formula):
    """Parse a formula, then return the result of evaluating it against a row.
    @param row: the HXL row object
    @param formula: the formula as a string
    @return: a scalar result
    """
    statement = p.parser.parse(formula, lexer=l.lexer)
    if statement:
        return statement[0](row, statement[1])
    else:
        logup('Cannot parse formula', {"formula": formula}, level='error')
        logger.error("Cannot parse formula {{ {} }}".format(formula))
        return "**ERROR**"

