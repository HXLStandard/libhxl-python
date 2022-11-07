
import logging
import ply.lex as lex, json

from hxl.util import logup

logger = logging.getLogger(__name__)

tokens = (
    'NAME',
    'TAGPATTERN',
    'INT',
    'FLOAT',
    'STRING',
    'PLUS',
    'MINUS',
    'TIMES',
    'DIVIDE',
    'MODULO',
    'LPAREN',
    'RPAREN',
    'COMMA'
)

t_ignore = " \t\r\n"

# Regular expression rules for simple tokens
t_NAME = r'[A-Za-z][A-Za-z0-9_]*'
t_TAGPATTERN = r'\#[A-Za-z][A-Za-z0-9_]*(\s*[+-][A-Za-z][A-Za-z0-9_]*)*[!]?'
t_PLUS = r'\+'
t_MINUS = r'-'
t_TIMES = r'\*'
t_DIVIDE = r'/'
t_MODULO = r'%'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_COMMA = r','

def t_STRING(t):
    r'"([^"]|\\.)*"'
    t.value = json.loads(t.value)
    return t

def t_FLOAT(t):
    r'\d+\.\d+'
    t.value = float(t.value)
    return t

def t_INT(t):
    r'\d+'
    t.value = int(t.value)
    return t

def t_error(t):
    logup('Illegal character', {"char": t.value[0]}, level='error')
    logger.error("Illegal character '%s'", t.value[0])
    t.lexer.skip(1)

lexer = lex.lex()
