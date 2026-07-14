"""
Transformação - Camada Silver: Itens de Pedido (order_items)
================================================================
Regras aplicadas:
- Conversão de shipping_limit_date para timestamp
- Remoção de registros com price ou freight_value negativos
  (validação de regra de domínio: preço/frete negativo é logicamente
  impossível, não é um "nulo aceitável" como vimos em products)
- Remoção de duplicatas pela chave composta (order_id + order_item_id)
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
    spark = criar_spark_session("SilverTransformation-OrderItems")

    caminho_bronze = str(BRONZE_DIR / "order_items" / "order_items.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    df = df.filter(
        col("order_id").isNotNull() & col("order_item_id").isNotNull()
    )

    df = df.withColumn(
        "shipping_limit_date", col("shipping_limit_date").cast("timestamp")
    )

    df = df.filter((col("price") >= 0) & (col("freight_value") >= 0))

    df = df.dropDuplicates(["order_id", "order_item_id"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "order_items")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (order_items) concluida com sucesso.")


if __name__ == "__main__":
    main()