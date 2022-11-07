import logging
import ply.yacc as yacc
import hxl.model
import hxl.formulas.functions as f

from hxl.util import logup

logger = logging.getLogger(__name__)

from hxl.formulas.lexer import tokens

precedence = [
    ['left', 'PLUS', 'MINUS'],
    ['left', 'TIMES', 'DIVIDE', 'MODULO'],
    ['right', 'UMINUS'],
]

def p_expression_const(p):
    """expression : INT
                  | FLOAT
                  | STRING
    """
    p[0] = [f.const, [p[1]]]

def p_expression_group(p):
    'expression : LPAREN expression RPAREN'
    p[0] = p[2]

def p_expression_plus(p):
    'expression : expression PLUS expression'
    p[0] = [f.add, [p[1], p[3]]]

def p_expression_minus(p):
    'expression : expression MINUS expression'
    p[0] = [f.subtract, [p[1], p[3]]]

def p_expression_times(p):
    'expression : expression TIMES expression'
    p[0] = [f.multiply, [p[1], p[3]]]

def p_expression_divide(p):
    'expression : expression DIVIDE expression'
    p[0] = [f.divide, [p[1], p[3]]]

def p_expression_modulo(p):
    'expression : expression MODULO expression'
    p[0] = [f.modulo, [p[1], p[3]]]

def p_expression_uminus(p):
    'expression : MINUS expression %prec UMINUS'
    p[0] = [f.subtract, [0, p[2]]]

def p_expression_tagpattern(p):
    'expression : TAGPATTERN'
    p[0] = [f.tagref, [hxl.model.TagPattern.parse(p[1])]]

def p_expression_function(p):
    'expression : NAME LPAREN args RPAREN'
    p[0] = [f.function, [p[1]] + p[3]]

def p_args_multiple(p):
    'args : expression COMMA args'
    p[0] = [p[1]] + p[3]

def p_args_single(p):
    'args : expression'
    p[0] = [p[1]]

def p_args_empty(p):
    'args :'
    p[0] = []

# Error rule for syntax errors
def p_error(p):
    logup('Syntax error', {"err": str(p)}, level='error')
    logger.error("Syntax error: %s", str(p))

parser = yacc.yacc()
