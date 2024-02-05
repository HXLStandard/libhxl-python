""" Other misc utilities
"""

import logging
import os
import sys
import structlog

def logup(msg, props={}, level="notset"):
    """
    Adds the function name on the fly for the log

    Args:
        msg: the actual log message
        props: additional properties for the log

    """
    if level == 'notset':
        level = os.getenv('LOGGING_LEVEL', 'INFO').lower()
    input_logger = structlog.wrap_logger(logging.getLogger('hxl.REMOTE_ACCESS'))
    props['function'] = sys._getframe(1).f_code.co_name
    levels = {
        "critical": 50,
        "error": 40,
        "warning": 30,
        "info": 20,
        "debug": 10
    }
    input_logger.log(level=levels[level], event=msg, **props)
