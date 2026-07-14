"""
Transformação - Camada Silver: Avaliações de Pedido (order_reviews)
=======================================================================
Regras aplicadas:
- Conversão de datas para timestamp
- Remoção de duplicatas por review_id
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
    spark = criar_spark_session("SilverTransformation-OrderReviews")

    caminho_bronze = str(BRONZE_DIR / "order_reviews" / "order_reviews.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    df = df.filter(col("review_id").isNotNull())

    for nome_coluna in ["review_creation_date", "review_answer_timestamp"]:
        df = df.withColumn(nome_coluna, col(nome_coluna).cast("timestamp"))

    df = df.dropDuplicates(["review_id"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "order_reviews")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (order_reviews) concluida com sucesso.")


if __name__ == "__main__":
    main()