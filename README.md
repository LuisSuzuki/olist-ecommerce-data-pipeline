# Olist E-commerce Data Pipeline

Pipeline de engenharia de dados de ponta a ponta, construído sobre o dataset público **Brazilian E-Commerce (Olist)**, aplicando arquitetura Medallion (Bronze/Silver/Gold), orquestração com Apache Airflow, e consumo analítico via Power BI.

## Arquitetura

Kaggle API → Bronze (Parquet) → Silver (PySpark, Data Quality) → Gold (Star Schema)
→ PostgreSQL (Data Warehouse) → Power BI
Orquestrado por Apache Airflow (Docker)

## Stack Técnica

- **Ingestão**: Python, Kaggle API
- **Processamento**: Apache Spark / PySpark 4.1.2
- **Armazenamento**: Parquet (Data Lake local)
- **Modelagem**: Star Schema (4 dimensões + 1 fato)
- **Orquestração**: Apache Airflow 3.2.2 (Docker Compose, imagem customizada com Java 17)
- **Data Warehouse**: PostgreSQL 16
- **Visualização**: Power BI
- **Testes**: pytest
- **CI/CD**: GitHub Actions

## Estrutura do Repositório

├── data/ # Data Lake local (Bronze/Silver/Gold) - nao versionado
├── src/
│ ├── ingestion/ # Scripts de ingestao (Kaggle -> Bronze)
│ ├── transformation/ # Transformacoes PySpark (Silver, Gold)
│ ├── loading/ # Carga Gold -> PostgreSQL
│ └── utils/ # Modulos compartilhados
├── orchestration/ # Airflow (Docker Compose, Dockerfile, DAGs)
├── warehouse/ # PostgreSQL (Docker Compose)
├── reports/ # Dashboard Power BI
├── tests/ # Testes automatizados (pytest)
└── .github/workflows/ # CI/CD (GitHub Actions)

## Destaques Técnicos

- **Medallion Architecture**: separação clara entre dados brutos, limpos e prontos para negócio
- **Data Quality real**: identificação e tratamento de 26% de duplicatas na tabela de geolocalização, validação de regras de domínio (valores negativos), integridade referencial validada (zero nulos em surrogate keys)
- **Paralelismo controlado**: DAG do Airflow com paralelismo real entre tasks independentes, ajustado para os limites de recursos do ambiente local
- **Infraestrutura como código**: ambiente de orquestração totalmente reprodutível via Docker (imagem customizada com Java + PySpark)
- **Testes automatizados**: cobertura das principais regras de qualidade de dados, rodando em CI a cada push

## Como Executar

1. Clone o repositório
2. Configure o ambiente Python: `python -m venv venv && venv\Scripts\activate && pip install -r requirements.txt`
3. Configure sua credencial Kaggle (`kaggle.json`)
4. Suba a infraestrutura: `docker compose -f orchestration/docker-compose.yaml up -d` e `docker compose -f warehouse/docker-compose.yaml up -d`
5. Acesse o Airflow em `localhost:8080` e dispare a DAG `pipeline_olist_ecommerce`

## Autor

Luis Suzuki — Data Engineer
[LinkedIn] · [GitHub](https://github.com/LuisSuzuki)