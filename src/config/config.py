import os
from dotenv import load_dotenv

cur_dir = os.path.dirname(os.path.realpath(__file__))
load_dotenv(os.path.join(cur_dir, '.env'))


def get(str: str) -> str:
    got = os.getenv(str)
    if got is None:
        exit(f"got NoneType for env:{str}")
    return got
