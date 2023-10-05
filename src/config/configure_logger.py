import logging
import sys

def configure_logging():
    logging.basicConfig(
        level=logging.NOTSET,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)  # Logging to terminal
        ]
    )

def get_logger(name: str):
    return logging.getLogger(name)
