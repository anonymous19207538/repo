import logging

import colorlog

logger = logging.getLogger(__name__)


def _init_logger():
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)s: %(name)s %(message)s")
    )
    handler.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)


_init_logger()
