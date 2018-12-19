import logging
import ply.yacc as yacc

logger = logging.getLogger(__name__)

from hxleq.lexer import tokens

def p_expression_plus(p):
    'expression : expression PLUS expression'
    p[0] = ('add', p[1], p[3])

def p_expression_minus(p):
    'expression : expression MINUS expression'
    p[0] = ('subtract', p[1], p[3])

def p_expression_term(p):
    'expression : term'
    p[0] = p[1]

def p_term_times(p):
    'expression : term TIMES factor'
    p[0] = ('multiply', p[1], p[3])

def p_term_divide(p):
    'expression : term DIVIDE factor'
    p[0] = ('divide', p[1], p[3])

def p_term_modulo(p):
    'expression : term MODULO factor'
    p[0] = ('modulo', p[1], p[3])

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
    p[0] = ('ref', p[1])

def p_factor_uminus(p):
    'factor : MINUS factor'
    p[0] = ('minus', 0, p[2])

def p_factor_function(p):
    'factor : NAME LPAREN args RPAREN'
    p[0] = ('function', p[1], p[3])

def p_args_multiple(p):
    'args : TAGPATTERN COMMA args'
    p[0] = [p[1]] + p[3]

def p_args_single(p):
    'args : TAGPATTERN'
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
