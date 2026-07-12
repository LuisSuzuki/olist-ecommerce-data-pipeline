"""
Transformação - Camada Silver: Pedidos (orders)
=================================================
Lê a tabela 'orders' da camada Bronze (Parquet), aplica regras de
qualidade e padronização, e grava o resultado na camada Silver.

Regras aplicadas nesta camada:
- Conversão de colunas de data (string -> timestamp)
- Remoção de duplicatas por order_id
- Padronização do campo order_status
- Remoção de registros sem order_id (chave primária ausente)
"""

import sys
from pathlib import Path

# Adicionamos a pasta 'src' ao caminho de busca de módulos do Python,
# para podermos importar nosso módulo utilitário compartilhado a partir
# de qualquer lugar, independente de onde o script for executado.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, trim, lower
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


COLUNAS_DE_DATA = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def main() -> None:
    spark = criar_spark_session("SilverTransformation-Orders")

    caminho_bronze = str(BRONZE_DIR / "orders" / "orders.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)

    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    # --- Regra 1: remover registros sem order_id ---
    df = df.filter(col("order_id").isNotNull())

    # --- Regra 2: converter colunas de data de string para timestamp ---
    for nome_coluna in COLUNAS_DE_DATA:
        df = df.withColumn(nome_coluna, col(nome_coluna).cast("timestamp"))

    # --- Regra 3: padronizar order_status ---
    df = df.withColumn("order_status", lower(trim(col("order_status"))))

    # --- Regra 4: remover duplicatas por order_id ---
    df = df.dropDuplicates(["order_id"])

    linhas_depois = df.count()
    linhas_removidas = linhas_antes - linhas_depois
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_removidas} removidos por duplicidade/nulos)")

    # --- Gravando o resultado na camada Silver ---
    caminho_saida = str(SILVER_DIR / "orders")

    df.write.mode("overwrite").parquet(caminho_saida)

    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (orders) concluida com sucesso.")


if __name__ == "__main__":
    main()