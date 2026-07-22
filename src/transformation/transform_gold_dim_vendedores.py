"""
Transformação - Camada Gold: Dimensão Vendedores (dim_vendedores)
=====================================================================
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
    spark = criar_spark_session("GoldTransformation-DimVendedores")

    caminho_silver = str(SILVER_DIR / "sellers")
    print(f"Lendo dados da Silver: {caminho_silver}")

    df = spark.read.parquet(caminho_silver)

    df = df.select("seller_id", "seller_city", "seller_state")
    df = df.withColumn("sk_vendedor", monotonically_increasing_id())
    df = df.select("sk_vendedor", "seller_id", "seller_city", "seller_state")

    linhas = df.count()
    print(f"Dimensão dim_vendedores gerada com {linhas} registros")

    caminho_saida = str(GOLD_DIR / "dim_vendedores")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Gold salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Gold (dim_vendedores) concluida com sucesso.")


if __name__ == "__main__":
    main()