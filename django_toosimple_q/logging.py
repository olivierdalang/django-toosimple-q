import logging


handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

logger = logging.getLogger('toosimpleq')
logger.addHandler(handler)
