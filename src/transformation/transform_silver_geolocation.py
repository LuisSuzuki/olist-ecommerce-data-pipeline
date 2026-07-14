"""
Transformação - Camada Silver: Geolocalização (geolocation)
===============================================================
Regras aplicadas:
- Remoção de duplicatas de linha completa (esta tabela não tem uma
  chave primária única de negócio; múltiplos CEPs podem legitimamente
  compartilhar coordenadas, mas linhas 100% idênticas são redundância
  pura vinda da fonte)
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.spark_session import criar_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def main() -> None:
    spark = criar_spark_session("SilverTransformation-Geolocation")

    caminho_bronze = str(BRONZE_DIR / "geolocation" / "geolocation.parquet")
    print(f"Lendo dados da Bronze: {caminho_bronze}")

    df = spark.read.parquet(caminho_bronze)
    linhas_antes = df.count()
    print(f"Registros lidos da Bronze: {linhas_antes}")

    # dropDuplicates() SEM argumentos considera a linha inteira (todas
    # as colunas) para decidir o que é duplicata — diferente das outras
    # tabelas, onde especificamos só a(s) coluna(s) de chave primária.
    df = df.dropDuplicates()

    linhas_depois = df.count()
    print(f"Registros após limpeza: {linhas_depois} "
          f"({linhas_antes - linhas_depois} removidos)")

    caminho_saida = str(SILVER_DIR / "geolocation")
    df.write.mode("overwrite").parquet(caminho_saida)
    print(f"Dados da Silver salvos em: {caminho_saida}")

    spark.stop()
    print("\nTransformacao Silver (geolocation) concluida com sucesso.")


if __name__ == "__main__":
    main()