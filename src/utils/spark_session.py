"""
Módulo utilitário compartilhado para criação da SparkSession.

Centralizamos essa lógica aqui para que todos os scripts de
transformação do projeto configurem o Spark de forma idêntica e
consistente, sem duplicar código.
"""

import os
import sys

from pyspark.sql import SparkSession


def criar_spark_session(nome_app: str) -> SparkSession:
    """
    Cria (ou reutiliza) uma SparkSession configurada corretamente
    para rodar em ambiente local no Windows.

    Parâmetros
    ----------
    nome_app : str
        Nome identificador da aplicação Spark, usado em logs e na
        interface de monitoramento. Cada script de transformação
        passa seu próprio nome (ex: "SilverTransformation-Orders").

    Retorna
    -------
    SparkSession
        A sessão Spark pronta para uso.
    """

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName(nome_app)
        .master("local[*]")
        .getOrCreate()
    )