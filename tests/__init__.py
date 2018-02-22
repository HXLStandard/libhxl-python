import logging

# Default to turning off all but critical logging messages
logging.basicConfig(level=logging.CRITICAL)

def resolve_path(filename):
    """Resolve a pathname for a test input file."""
    return os.path.join(os.path.dirname(__file__), filename)

