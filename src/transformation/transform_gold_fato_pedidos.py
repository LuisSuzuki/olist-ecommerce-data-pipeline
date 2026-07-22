"""
Transformação - Camada Gold: Fato Pedidos (fato_pedidos)
============================================================
Tabela fato central do Star Schema. Granularidade: um item de
pedido por linha.

Constrói o fato juntando order_items + orders (Silver) com as
quatro dimensões já geradas (dim_clientes, dim_produtos,
dim_vendedores, dim_tempo), substituindo cada chave natural pela
surrogate key correspondente.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, to_date
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    spark = criar_spark_session("GoldTransformation-FatoPedidos")

    print("Lendo tabelas Silver (order_items, orders) e dimensoes Gold")

    df_items = spark.read.parquet(str(SILVER_DIR / "order_items"))
    df_orders = spark.read.parquet(str(SILVER_DIR / "orders"))

    dim_clientes = spark.read.parquet(str(GOLD_DIR / "dim_clientes"))
    dim_produtos = spark.read.parquet(str(GOLD_DIR / "dim_produtos"))
    dim_vendedores = spark.read.parquet(str(GOLD_DIR / "dim_vendedores"))
    dim_tempo = spark.read.parquet(str(GOLD_DIR / "dim_tempo"))

    linhas_iniciais = df_items.count()
    print(f"Registros iniciais (order_items): {linhas_iniciais}")

    df = df_items.join(
        df_orders.select("order_id", "customer_id", "order_purchase_timestamp"),
        on="order_id",
        how="left",
    )

    df = df.join(
        dim_clientes.select("customer_id", "sk_cliente"),
        on="customer_id",
        how="left",
    )

    df = df.join(
        dim_produtos.select("product_id", "sk_produto"),
        on="product_id",
        how="left",
    )

    df = df.join(
        dim_vendedores.select("seller_id", "sk_vendedor"),
        on="seller_id",
        how="left",
    )

    df = df.withColumn(
        "data_compra", to_date(col("order_purchase_timestamp"))
    )

    df = df.join(
        dim_tempo.select(col("data").alias("data_compra"), "sk_tempo"),
        on="data_compra",
        how="left",
    )

    df_fato = df.select(
        "sk_cliente",
        "sk_produto",
        "sk_vendedor",
        "sk_tempo",
        "order_id",
        "order_item_id",
        col("price").alias("valor_item"),
        col("freight_value").alias("valor_frete"),
    )

    linhas_finais = df_fato.count()
    print(f"Registros finais (fato_pedidos): {linhas_finais}")

    colunas_sk = ["sk_cliente", "sk_produto", "sk_vendedor", "sk_tempo"]

    for coluna in colunas_sk:
        nulos = df_fato.filter(col(coluna).isNull()).count()
        if nulos > 0:
            print(f"  AVISO: {nulos} registros com '{coluna}' nulo "
                  f"(join sem correspondencia)")
        else:
            print(f"  OK: nenhum nulo em '{coluna}'")

    caminho_saida = str(GOLD_DIR / "fato_pedidos")
    df_fato.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Gold salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Gold (fato_pedidos) concluida com sucesso.")


if __name__ == "__main__":
    main()