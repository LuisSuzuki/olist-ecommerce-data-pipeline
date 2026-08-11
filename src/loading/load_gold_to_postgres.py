"""
Carga - Gold (Parquet) -> PostgreSQL (Data Warehouse)
========================================================
Le cada tabela da camada Gold (arquivos Parquet, possivelmente
particionados em multiplos arquivos pelo Spark) e carrega no
PostgreSQL, table a tabela, substituindo o conteudo a cada execucao
(garante idempotencia, mesmo principio aplicado em todas as fases
anteriores).
"""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

def carregar_variaveis_env(caminho_env: Path) -> dict:
    variaveis = {}
    with open(caminho_env, "r") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if linha and not linha.startswith("#") and "=" in linha:
                chave, valor = linha.split("=", 1)
                variaveis[chave] = valor
    return variaveis


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
ENV_PATH = PROJECT_ROOT / "warehouse" / ".env"

TABELAS_GOLD = [
    "dim_clientes", "dim_produtos", "dim_vendedores", "dim_tempo",
    "fato_pedidos",
]


def main() -> None:
    env = carregar_variaveis_env(ENV_PATH)

    url_conexao = (
        f"postgresql+psycopg2://{env['POSTGRES_USER']}:"
        f"{env['POSTGRES_PASSWORD']}@localhost:{env['POSTGRES_PORT']}/"
        f"{env['POSTGRES_DB']}"
    )

    engine = create_engine(url_conexao)

    for nome_tabela in TABELAS_GOLD:
        caminho_parquet = GOLD_DIR / nome_tabela

        print(f"Lendo: {caminho_parquet}")

        df = pd.read_parquet(caminho_parquet, engine="pyarrow")

        print(f"  {len(df)} linhas lidas. Carregando na tabela '{nome_tabela}'...")

        df.to_sql(
            nome_tabela,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )

        print(f"  Tabela '{nome_tabela}' carregada com sucesso.\n")

    print("Carga completa da camada Gold no PostgreSQL concluida.")


if __name__ == "__main__":
    main()