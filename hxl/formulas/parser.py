import logging
import ply.yacc as yacc
import hxl.model
import hxl.formulas.functions as f

logger = logging.getLogger(__name__)

from hxl.formulas.lexer import tokens

def p_expression_plus(p):
    'expression : expression PLUS expression'
    p[0] = (f.add, (p[1], p[3]))

def p_expression_minus(p):
    'expression : expression MINUS expression'
    p[0] = (f.subtract, (p[1], p[3]))

def p_expression_term(p):
    'expression : term'
    p[0] = p[1]

def p_term_times(p):
    'expression : term TIMES factor'
    p[0] = (f.multiply, (p[1], p[3]))

def p_term_divide(p):
    'expression : term DIVIDE factor'
    p[0] = (f.divide, (p[1], p[3]))

def p_term_modulo(p):
    'expression : term MODULO factor'
    p[0] = (f.modulo, (p[1], p[3]))

def p_term_factor(p):
    'term : factor'
    p[0] = p[1]

def p_factor_int(p):
    'factor : INT'
    p[0] = p[1]

def p_factor_float(p):
    'factor : FLOAT'
    p[0] = p[1]

def p_factor_tagpattern(p):
    'factor : TAGPATTERN'
    p[0] = (f.ref, [hxl.model.TagPattern.parse(p[1])])

def p_factor_uminus(p):
    'factor : MINUS factor'
    p[0] = (f.substract, (0, p[2]))

def p_factor_function(p):
    'factor : NAME LPAREN args RPAREN'
    p[0] = (f.function, [p[1]] + p[3])

def p_args_multiple(p):
    'args : factor COMMA args'
    p[0] = [p[1]] + p[3]

def p_args_single(p):
    'args : factor'
    p[0] = [p[1]]

def p_args_empty(p):
    'args :'
    p[0] = []

def p_factor_group(p):
    'factor : LPAREN expression RPAREN'
    p[0] = p[2]

# Error rule for syntax errors
def p_error(p):
    logger.error("Syntax error: %s", str(p))

parser = yacc.yacc()
