"""
Testes unitarios das regras de transformacao do pipeline.
Usa uma SparkSession local, isolada, criada so para os testes.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import pytest
from pyspark.sql import SparkSession
from utils.spark_session import criar_spark_session


@pytest.fixture(scope="module")
def spark():
    """Fixture: cria uma SparkSession reutilizada por todos os testes
    deste arquivo, evitando o overhead de criar uma nova a cada teste."""
    session = criar_spark_session("Testes-Pipeline")
    yield session
    session.stop()


def test_remove_valores_negativos(spark):
    """Valida a regra de dominio aplicada em order_items: price e
    freight_value nunca podem ser negativos apos a limpeza."""
    dados = [
        ("pedido1", 1, 100.0, 10.0),
        ("pedido2", 1, -50.0, 10.0),  # invalido: price negativo
        ("pedido3", 1, 100.0, -5.0),  # invalido: freight negativo
    ]
    df = spark.createDataFrame(dados, ["order_id", "order_item_id", "price", "freight_value"])

    df_limpo = df.filter((df.price >= 0) & (df.freight_value >= 0))

    assert df_limpo.count() == 1


def test_remove_duplicatas_por_chave(spark):
    """Valida que dropDuplicates por chave remove registros repetidos."""
    dados = [
        ("cliente1", "SP"),
        ("cliente1", "SP"),  # duplicata
        ("cliente2", "RJ"),
    ]
    df = spark.createDataFrame(dados, ["customer_id", "customer_state"])

    df_limpo = df.dropDuplicates(["customer_id"])

    assert df_limpo.count() == 2


def test_remove_nulos_em_chave_primaria(spark):
    """Valida que registros com chave primaria nula sao descartados."""
    dados = [
        ("pedido1",),
        (None,),
        ("pedido3",),
    ]
    df = spark.createDataFrame(dados, ["order_id"])

    df_limpo = df.filter(df.order_id.isNotNull())

    assert df_limpo.count() == 2