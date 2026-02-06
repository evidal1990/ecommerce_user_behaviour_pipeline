from dotenv import load_dotenv
import os


def load():
    load_dotenv()
    environment = os.getenv("ENV")
    if not environment:
        raise EnvironmentError("A variável de ambiente 'ENV' não está definida.")
    return environment
