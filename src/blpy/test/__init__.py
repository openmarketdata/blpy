import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)-8s %(name)-12s %(message)s',
)
logger = logging.getLogger(__name__)
from src import blpy

pipe=blpy.Connection(logger=logger)
