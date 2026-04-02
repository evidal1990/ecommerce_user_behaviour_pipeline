# Pipeline de Dados para Análise de Comportamento de Usuários de E-Commerce

## Objetivo

O projeto tem como foco automatizar o fluxo de ingestão de dados provenientes de diferentes fontes, aplicar processos de transformação e disponibilizar dados estruturados e confiáveis para análise. Dessa forma, os dados permanecem atualizados, consistentes e prontos para suportar decisões orientadas por dados em contextos de E-commerce.

O desenvolvimento do projeto iniciou com a definição do dicionário de dados, levantamento de requisitos de negócio e definição de KPIs. A partir disso, foi desenhada a arquitetura do pipeline, estruturando o fluxo de ingestão, transformação e armazenamento dos dados.

Após a conclusão do pipeline, os dados serão utilizados para análises exploratórias, desenvolvimento de modelos de Machine Learning e criação de dashboards, permitindo extrair insights sobre o comportamento dos usuários e apoiar estratégias de negócio.

## KPIS Descritivos

- % de usuários por faixa etária
- % de usuários por gênero
- % de usuários por país
- % de usuários por contexto urbano x rural
- % de usuários por faixa de renda
- % de usuários por nível de escolaridade
- % de usuários por status de emprego
- % de usuários por tipo de dispositivo
- % de usuários com filhos

## KPIS Comportamentais

- % de usuários que adotam a assinatura premium 
- Tempo médio diário na plataforma
- Frequência média de uso da plataforma por semana
- Média de produtos vistos por dia
- Categoria de produtos preferida mais populares
- Meio de pagamento preferido

## KPIS Operacionais

- Média de lealdade à marca
- Média de dependência de cupom de desconto
- Média de atividade de referência

## KPIS Estratégicos

- Taxa de conversão
- Taxa de abandono de carrinho
- Taxa de churn
- DAU
- Score de engajamento
- NPS

## Link para a Documentação do Projeto
https://drive.google.com/file/d/1To4Z_KnayDgVKSdtN9Zu-NozFfFH2g0c/view?usp=drive_link

## Arquitetura
RAW -> BRONZE -> SILVER -> GOLD

### Camada RAW
Armazena os dados exatamente como foram recebidos, sem nenhum tipo de modificação.

### Camada BRONZE
Transforma dados brutos em dadoss tecnicamente utilizáveis, sem lógica de negócio.

### Camada SILVER
Os dados passam a representar a realidade do negócio.

### Camada GOLD
Armazena os dados prontos para consumo (análises).

## Stack
- Python
- Polars
- Google Cloud Storage

## Estrutura de Pastas

```
.
├── config
├── consts
├── credentials
├── data
│   ├── bronze
│   ├── gold
│   ├── raw
│   └── silver
├── database
├── docker
├── logs
├── scripts
├── src
│   ├── ingestion
│   ├── load
│   ├── orchestration
│   │   ├── config
│   │   └── executors
│   ├── transformation
│   │   ├── bronze
│   │   │   └── fixes
│   │   ├── gold
│   │   │   ├── aggregate
│   │   │   └── metrics
│   │   └── silver
│   │       ├── clean
│   │       ├── enrich
│   │       └── normalize
│   ├── utils
│   └── validation
│       ├── business
│       ├── quality
│       ├── rules
│       └── semantic
├── storage
├── tests
│   ├── data
│   ├── dataframe
│   ├── file_io
│   ├── gcp
│   ├── ingestion
│   ├── load
│   ├── results
│   ├── transformation
│   └── validation
```

## Link do Kaggle
https://www.kaggle.com/datasets/dhrubangtalukdar/e-commerce-shopper-behavior-amazonshopify-based

## Como Executar o Pipeline
**O CSV será baixado automaticamente do Kaggle através do link presente em `config/settings.yaml`**

1. Certifique-se de ter as dependências instaladas 
(verifique o arquivo `requirements.txt` para obter a lista de dependências)
2. Execute o pipeline executando o comando `python scripts/run_pipeline.py` no terminal.

## Como Executar os Testes

1. Certifique-se de ter as dependências instaladas.
(verifique o arquivo `requirements.txt` para obter a lista de dependências)
2. Execute os testes executando o comando `pytest -vv` no terminal.
(isso executará todos os testes no diretório `tests`).

## Descrição do Pipeline

O pipeline é composto por três camadas: raw, bronze, silver e gold. 
O pipeline é executado pelo arquivo `pipeline.py`, que carrega as configurações do pipeline, cria um objeto `Pipeline` e executa o método `run()`.
**O resultado de cada etapa é registrado em um arquivo de log.**

