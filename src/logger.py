import logging
import sys

logger = logging.getLogger("DATA_PIPELINE")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(level=logging.INFO)
formatter = logging.Formatter("[%(name)s - %(asctime)s - %(levelname)s] => %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
