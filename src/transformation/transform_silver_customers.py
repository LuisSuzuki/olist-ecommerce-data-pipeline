"""
Transformação - Camada Silver: Clientes (customers)
======================================================
Regras aplicadas:
- Remoção de duplicatas por customer_id
- Padronização de customer_state (maiúsculas, sem espaços)
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
    spark = criar_spark_session("SilverTransformation-Customers")

    caminho_bronze = str(BRONZE_DIR / "customers" / "customers.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    # Remove clientes sem identificador (chave primária ausente)
    df = df.filter(col("customer_id").isNotNull())

    # Padroniza a sigla do estado: remove espaços extras e força maiúsculas
    df = df.withColumn("customer_state", upper(trim(col("customer_state"))))

    # Remove duplicatas pela chave primária
    df = df.dropDuplicates(["customer_id"])

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "customers")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (customers) concluida com sucesso.")


if __name__ == "__main__":
    main()