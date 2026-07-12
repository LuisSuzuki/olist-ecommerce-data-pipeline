"""
Transformação - Camada Silver: Produtos (products)
=====================================================
Regras aplicadas:
- Remoção de duplicatas por product_id
- Preenchimento de categoria ausente com valor padrão "nao_informado"
  (decisão de negócio: produto sem categoria ainda é um produto válido,
  não deve ser descartado, apenas sinalizado)
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
    spark = criar_spark_session("SilverTransformation-Products")

    caminho_bronze = str(BRONZE_DIR / "products" / "products.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    df = df.filter(col("product_id").isNotNull())

    # Trata valores nulos, substituindo por ""nao_informado"
    df = df.fillna({"product_category_name": "nao_informado"})

    df = df.dropDuplicates(["product_id"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "products")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (products) concluida com sucesso.")


if __name__ == "__main__":
    main()