import logging
import sys

def configure_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(filename)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)  # Logging to terminal
        ]
    )

def get_logger(name: str):
    configure_logging() 
    return logging.getLogger(name)
