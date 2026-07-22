"""
Transformação - Camada Silver: Tradução de Categoria de Produto
===================================================================
Tabela pequena de referência (produto_categoria_pt -> produto_categoria_en).
Regra aplicada: remoção de duplicatas pela chave de negócio
(product_category_name).
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
    spark = criar_spark_session("SilverTransformation-ProductCategoryTranslation")

    caminho_bronze = str(
        BRONZE_DIR / "product_category_name_translation"
        / "product_category_name_translation.parquet"
    )
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    df = df.filter(col("product_category_name").isNotNull())
    df = df.dropDuplicates(["product_category_name"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "product_category_name_translation")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (product_category_name_translation) concluida com sucesso.")


if __name__ == "__main__":
    main()