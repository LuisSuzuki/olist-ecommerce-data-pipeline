"""
Transformação - Camada Gold: Dimensão Tempo (dim_tempo)
===========================================================
Gera uma linha para cada dia entre 2016-01-01 e 2018-12-31 (período
que cobre com folga todas as datas de pedidos do dataset), com
colunas de ano, mês, dia, trimestre e dia da semana pré-calculados.

Diferente das outras dimensões, esta não lê de uma tabela Silver —
é construída matematicamente, prática padrão para dimensões de tempo
em Data Warehousing.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import (
    sequence, explode, to_date, year, month, dayofmonth,
    quarter, dayofweek, date_format, monotonically_increasing_id, lit,
)
from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

DATA_INICIO = "2016-01-01"
DATA_FIM = "2018-12-31"


def main() -> None:
    spark = criar_spark_session("GoldTransformation-DimTempo")

    print(f"Gerando dimensao de tempo de {DATA_INICIO} ate {DATA_FIM}")

    df = spark.createDataFrame([(1,)], ["seed"])

    df = df.withColumn(
        "data",
        explode(sequence(
            to_date(lit(DATA_INICIO)),
            to_date(lit(DATA_FIM)),
        ))
    )

    df = df.withColumn("ano", year("data"))
    df = df.withColumn("mes", month("data"))
    df = df.withColumn("dia", dayofmonth("data"))
    df = df.withColumn("trimestre", quarter("data"))

    df = df.withColumn("dia_semana_num", dayofweek("data"))
    df = df.withColumn("nome_dia_semana", date_format("data", "EEEE"))
    df = df.withColumn("nome_mes", date_format("data", "MMMM"))

    df = df.withColumn(
        "fim_de_semana",
        (df["dia_semana_num"] == 1) | (df["dia_semana_num"] == 7)
    )

    df = df.withColumn("sk_tempo", monotonically_increasing_id())

    df = df.select(
        "sk_tempo", "data", "ano", "mes", "dia", "trimestre",
        "nome_mes", "dia_semana_num", "nome_dia_semana", "fim_de_semana",
    )

    linhas = df.count()
    print(f"Dimensão dim_tempo gerada com {linhas} registros (dias)")

    caminho_saida = str(GOLD_DIR / "dim_tempo")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Gold salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Gold (dim_tempo) concluida com sucesso.")


if __name__ == "__main__":
    main()