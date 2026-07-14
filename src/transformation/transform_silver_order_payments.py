"""
Transformação - Camada Silver: Pagamentos de Pedido (order_payments)
========================================================================
Regras aplicadas:
- Remoção de registros com payment_value negativo
- Remoção de duplicatas pela chave composta (order_id + payment_sequential)
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def main() -> None:
    spark = criar_spark_session("SilverTransformation-OrderPayments")

    caminho_bronze = str(BRONZE_DIR / "order_payments" / "order_payments.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    df = df.filter(
        col("order_id").isNotNull() & col("payment_sequential").isNotNull()
    )

    df = df.filter(col("payment_value") >= 0)

    df = df.dropDuplicates(["order_id", "payment_sequential"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "order_payments")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (order_payments) concluida com sucesso.")


if __name__ == "__main__":
    main()