from decouple import config


def get(str: str) -> str:
    return config(str)
