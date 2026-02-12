import logging
from pathlib import Path
from src.orchestration.pipeline import Pipeline
from src.utils import file_io

BASE_DIR = Path(__file__).resolve().parents[1]


def main() -> None:
    
    """
    Função principal que executa o pipeline.
    Carrega as configurações do pipeline, cria um objeto e executa o método run().
    Em caso de erro, registra a falha no log e levanta uma exceção.
    Retorno: 
        None
    """
    log_path = BASE_DIR / "logs" / "pipeline.log"
    log_format = "%(asctime)s | %(levelname)s | %(message)s"

    logging.basicConfig(
        level=logging.INFO, format=log_format, filename=log_path, filemode="w"
    )

    try:
        logging.info("Iniciando o pipeline...")
        settings = file_io.read_yaml(BASE_DIR / "config" / "settings.yaml")
        Pipeline(settings).run()
        logging.info("Pipeline finalizada com sucesso.")

    except Exception:
        logging.exception("Falha na execução do pipeline")
        raise


if __name__ == "__main__":
    main()
