"""
Transformação - Camada Gold: Dimensão Produtos (dim_produtos)
=================================================================
Lê a tabela Silver de produtos e a tabela de tradução de categorias,
junta as duas (JOIN), e gera a dimensão final com o nome da categoria
em português e em inglês.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import monotonically_increasing_id, col
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    spark = criar_spark_session("GoldTransformation-DimProdutos")

    print("Lendo dados da Silver: products, product_category_name_translation")

    df_produtos = spark.read.parquet(str(SILVER_DIR / "products"))
    df_traducao = spark.read.parquet(
        str(SILVER_DIR / "product_category_name_translation")
    )

    df = df_produtos.join(
        df_traducao,
        on="product_category_name",  
        how="left",
    )

    df = df.select(
        col("product_id"),
        col("product_category_name").alias("categoria_produto_pt"),
        col("product_category_name_english").alias("categoria_produto_en"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
    )

    df = df.withColumn("sk_produto", monotonically_increasing_id())

    df = df.select(
        "sk_produto", "product_id", "categoria_produto_pt", "categoria_produto_en",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    )

    linhas = df.count()
    print(f"Dimensão dim_produtos gerada com {linhas} registros")

    caminho_saida = str(GOLD_DIR / "dim_produtos")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Gold salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Gold (dim_produtos) concluida com sucesso.")


if __name__ == "__main__":
    main()