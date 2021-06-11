import logging

formatter = logging.Formatter(
    fmt="[%(asctime)s %(levelname)s] [toosimpleq] %(message)s"
)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

logger = logging.getLogger("toosimpleq")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
