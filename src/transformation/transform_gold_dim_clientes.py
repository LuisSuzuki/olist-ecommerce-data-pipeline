"""
Transformação - Camada Gold: Dimensão Clientes (dim_clientes)
=================================================================
Lê a tabela Silver de clientes e gera a dimensão final, com:
- Surrogate Key (sk_cliente) gerada artificialmente
- Apenas as colunas relevantes para análise (removendo metadados
  técnicos de ingestão que não interessam ao consumidor final)
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import monotonically_increasing_id
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    spark = criar_spark_session("GoldTransformation-DimClientes")

    caminho_silver = str(SILVER_DIR / "customers")
    print(f"Lendo dados da Silver: {caminho_silver}")

    df = spark.read.parquet(caminho_silver)

    df = df.select(
        "customer_id",
        "customer_city",
        "customer_state",
    )

    df = df.withColumn("sk_cliente", monotonically_increasing_id())

    df = df.select("sk_cliente", "customer_id", "customer_city", "customer_state")

    linhas = df.count()
    print(f"Dimensão dim_clientes gerada com {linhas} registros")

    caminho_saida = str(GOLD_DIR / "dim_clientes")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Gold salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Gold (dim_clientes) concluida com sucesso.")


if __name__ == "__main__":
    main()