"""
Script de Ingestão - Camada Bronze
====================================
Este script lê os arquivos CSV brutos (landing zone) do dataset Olist
e os converte para o formato Parquet, adicionando metadados técnicos
de rastreabilidade, sem aplicar nenhuma lógica de negócio ou limpeza.

Princípio da camada Bronze: preservar os dados o mais fiel possível
à origem, apenas mudando o formato de armazenamento para um mais
eficiente (Parquet), e adicionando contexto sobre QUANDO e DE ONDE
o dado veio.
"""

# --- Importação das bibliotecas ---

from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

# --- Configuração de caminhos ---

# Path(__file__) representa o caminho deste próprio arquivo .py.
# .resolve() transforma isso num caminho absoluto (completo), evitando
# problemas de caminho relativo dependendo de onde o script é executado.
# .parents[2] sobe 3 níveis de pasta: de src/ingestion/ingest_bronze.py
# até a raiz do projeto (src/ingestion -> src -> raiz do projeto).
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Pasta onde estão os CSVs originais (landing zone)
LANDING_DIR = PROJECT_ROOT / "data" / "bronze" / "landing"

# Pasta onde vamos salvar o resultado processado em Parquet
BRONZE_OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze"


def nome_tabela_a_partir_do_arquivo(caminho_csv: Path) -> str:
    """
    Converte o nome de um arquivo CSV da Olist em um nome de tabela
    mais limpo e padronizado.

    Exemplo: 'olist_customers_dataset.csv' -> 'customers'

    Por que fazemos isso: os nomes originais têm prefixos/sufixos
    repetitivos ('olist_' e '_dataset') que não agregam informação.
    Um nome de tabela limpo deixa o restante do pipeline mais legível.
    """
    # .stem retorna o nome do arquivo sem a extensão
    # (ex: 'olist_customers_dataset.csv' -> 'olist_customers_dataset')
    nome = caminho_csv.stem

    # .replace() remove os trechos que não queremos no nome final
    nome = nome.replace("olist_", "").replace("_dataset", "")

    return nome


def ingerir_arquivo(caminho_csv: Path) -> None:
    """
    Processa um único arquivo CSV: lê, adiciona metadados técnicos,
    e escreve o resultado em Parquet.
    """
    nome_tabela = nome_tabela_a_partir_do_arquivo(caminho_csv)

    print(f"Processando: {caminho_csv.name} -> tabela '{nome_tabela}'")

    df = pd.read_csv(caminho_csv)

    df["_ingestion_timestamp"] = datetime.now(timezone.utc)

    df["_source_file"] = caminho_csv.name

    # Criamos a pasta de destino para esta tabela específica, caso
    # ainda não exista. parents=True cria pastas intermediárias
    # necessárias; exist_ok=True evita erro se a pasta já existir.
    pasta_destino = BRONZE_OUTPUT_DIR / nome_tabela
    pasta_destino.mkdir(parents=True, exist_ok=True)

    caminho_saida = pasta_destino / f"{nome_tabela}.parquet"

    df.to_parquet(caminho_saida, engine="pyarrow", index=False)

    print(f"  -> Salvo em: {caminho_saida} ({len(df)} linhas)")


def main() -> None:
    """
    Função principal: encontra todos os CSVs na landing zone
    e processa cada um deles.
    """

    arquivos_csv = list(LANDING_DIR.glob("*.csv"))

    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado em {LANDING_DIR}")
        return

    print(f"Encontrados {len(arquivos_csv)} arquivos para processar.\n")

    for caminho_csv in arquivos_csv:
        ingerir_arquivo(caminho_csv)

    print("\nIngestão da camada Bronze concluída com sucesso.")


if __name__ == "__main__":
    main()