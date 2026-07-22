"""
DAG - Pipeline completo Olist E-commerce
===========================================
Orquestra o pipeline de dados de ponta a ponta:
Bronze (ingestao) -> Silver (limpeza) -> Gold (Star Schema)

Estrutura de dependencias:
1. Ingestao Bronze (todas as 9 tabelas de uma vez)
2. Transformacoes Silver (9 tabelas, em paralelo)
3. Dimensoes Gold (4 dimensoes, em paralelo)
4. Fato Gold (depende de todas as dimensoes)
"""

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import cross_downstream

SRC_PATH = "/opt/airflow/project/src"


@dag(
    dag_id="pipeline_olist_ecommerce",
    description="Pipeline Bronze -> Silver -> Gold do dataset Olist",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=3,
    tags=["olist", "portfolio", "medallion-architecture"],
)
def pipeline_olist_ecommerce():

    # --- Camada Bronze ---
    ingestao_bronze = BashOperator(
        task_id="ingestao_bronze",
        bash_command=f"python {SRC_PATH}/ingestion/ingest_bronze.py",
    )

    # --- Camada Silver (9 tabelas) ---
    tabelas_silver = [
        "orders", "customers", "products", "sellers", "order_items",
        "order_payments", "order_reviews", "geolocation",
        "product_category_translation",
    ]

    tasks_silver = []
    for nome_tabela in tabelas_silver:
        task_silver = BashOperator(
            task_id=f"silver_{nome_tabela}",
            bash_command=(
                f"python {SRC_PATH}/transformation/"
                f"transform_silver_{nome_tabela}.py"
            ),
        )
        tasks_silver.append(task_silver)

    # --- Camada Gold: Dimensoes (4 dimensoes) ---
    dimensoes_gold = ["clientes", "produtos", "vendedores", "tempo"]

    tasks_dimensoes = []
    for nome_dimensao in dimensoes_gold:
        task_dimensao = BashOperator(
            task_id=f"gold_dim_{nome_dimensao}",
            bash_command=(
                f"python {SRC_PATH}/transformation/"
                f"transform_gold_dim_{nome_dimensao}.py"
            ),
        )
        tasks_dimensoes.append(task_dimensao)

    # --- Camada Gold: Fato ---
    fato_pedidos = BashOperator(
        task_id="gold_fato_pedidos",
        bash_command=f"python {SRC_PATH}/transformation/transform_gold_fato_pedidos.py",
    )

    # --- Definindo as dependencias ---
    ingestao_bronze >> tasks_silver

    cross_downstream(tasks_silver, tasks_dimensoes)

    tasks_dimensoes >> fato_pedidos

pipeline_olist_ecommerce()