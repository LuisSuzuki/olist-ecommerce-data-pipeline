"""
Transformação - Camada Silver: Vendedores (sellers)
======================================================
Regras aplicadas:
- Remoção de duplicatas por seller_id
- Padronização de seller_state (maiúsculas, sem espaços)
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, trim, upper
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def main() -> None:
    spark = criar_spark_session("SilverTransformation-Sellers")

    caminho_bronze = str(BRONZE_DIR / "sellers" / "sellers.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    df = df.filter(col("seller_id").isNotNull())
    df = df.withColumn("seller_state", upper(trim(col("seller_state"))))
    df = df.dropDuplicates(["seller_id"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "sellers")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (sellers) concluida com sucesso.")


if __name__ == "__main__":
    main()