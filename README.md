# Pipeline de Dados para Análise de Comportamento de Usuários de E-Commerce

## Objetivo

Este pipeline foi desenvolvido para automatizar o processo de ingestão de dados (de diferentes fontes), transformar esses dados e armazená-los para análise. 
O objetivo é garantir que os dados estejam sempre atualizados e disponíveis para análise e tomada de decisões.

## Arquitetura
RAW -> BRONZE -> SILVER -> GOLD

## Stack
- Python
- Polars
- Google Cloud Storage

## Estrutura de Pastas

```
.
├── config
│   ├── connections.yaml
│   ├── google_cloud_platform.yaml
│   ├── loggin.yaml
│   └── settings.yaml
├── consts
│   └── dtypes.py
├── credentials
├── data
│   ├── bronze
│   ├── gold
│   ├── origin
│   ├── raw
│   └── silver
├── database
│   ├── bigquery.py
│   ├── engine.py
│   └── metadata.py
├── docker
│   ├── docker-compose.yml
│   └── Dockerfile
├── logs
├── pytest.ini
├── README.md
├── requirements.development.txt
├── requirements.production.txt
├── scripts
│   ├── backkfill.py
│   ├── cleanup.py
│   ├── run_pipeline.py
│   ├── seed_data.py
│   └── validate_data.py
├── src
│   ├── ingestion
│   │   ├── __init__.py
│   │   ├── csv_ingestion.py
│   │   └── schema.yaml
│   ├── load
│   │   ├── __init__.py
│   │   ├── load_gold.py
│   │   └── load_silver.py
│   ├── orchestration
│   │   ├── __init__.py
│   │   └── pipeline.py
│   ├── transformation
│   │   ├── bronze
│   │   │   ├── __init__.py
│   │   │   ├── clean.py
│   │   │   └── structure_data.py
│   │   └── silver
│   │       ├── __init__.py
│   │       ├── clean.py
│   │       ├── enrich.py
│   │       └── normalize.py
│   ├── utils
│   │   ├── dataframe.py
│   │   ├── db.py
│   │   ├── file_io.py
│   │   └── get_env_variables.py
│   └── validation
│       ├── __init__.py
│       └── quality_checks.py
├── storage
│   ├── __init__.py
│   └── gcs.py
└── tests
    ├── data
    ├── dataframe
    │   ├── __init__.py
    │   └── dataframe_test.py
    ├── file_io
    │   ├── __init__.py
    │   └── file_io_test.py
    ├── gcp
    │   ├── __init__.py
    │   ├── bucket_test.py
    │   └── connection_test.py
    ├── ingestion
    │   ├── __init__.py
    │   └── ingestion_test.py
    ├── load
    │   └── load.test.py
    ├── results
    ├── transformation
    │   └── transformation.test.py
    └── validation
        └── test.py
```

## Como Executar o Pipeline

1. Certifique-se de ter as dependências instaladas 
(verifique o arquivo `requirements.txt` para obter a lista de dependências)
3. Execute o pipeline executando o comando `python src/orchestration/pipeline.py` no terminal.

## Como Executar os Testes

1. Certifique-se de ter as dependências instaladas.
(verifique o arquivo `requirements.txt` para obter a lista de dependências)
2. Execute os testes executando o comando `pytest -vv` no terminal.
(isso executará todos os testes no diretório `tests`).

## Descrição do Pipeline

O pipeline é composto por três camadas: raw, bronze, silver e gold. 
O pipeline é executado pelo arquivo `pipeline.py`, que carrega as configurações do pipeline, cria um objeto `Pipeline` e executa o método `run()`.
O resultado de cada etapa é registrado em um arquivo de log.

### Camada raw
Armazena os dados exatamente como foram recebidos, sem nenhum tipo de modificação.

### Camada bronze
Transforma dados brutos em dadoss tecnicamente utilizáveis, sem lógica de negócio.

### Camada silver
Os dados passam a representar a realidade do negócio.

### Camada gold
Armazena os dados prontos para consumo (análises).

