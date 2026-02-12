from dotenv import load_dotenv
import os


def load():
    """
    Carrega as variáveis de ambiente do arquivo .env e retorna o valor da variável 'ENV'.

    Retorno:
        str: Valor da variável 'ENV' definida no arquivo .env.

    Raises:
        EnvironmentError: Se a variável 'ENV' não estiver definida.
    """
    load_dotenv()
    environment = os.getenv("ENV")
    if not environment:
        raise EnvironmentError("A variável de ambiente 'ENV' não está definida.")
    return environment
