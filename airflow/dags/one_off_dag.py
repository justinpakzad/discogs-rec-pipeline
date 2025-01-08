import datetime as dt
from airflow.models.dag import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator

with DAG(
    dag_id="discogs_rec_one_off_dag",
    start_date=dt.datetime(2024, 11, 20),
    catchup=False,
    schedule_interval=None,
    template_searchpath=["/opt/airflow/"],
) as dag:

    create_release_meta_dim = SQLExecuteQueryOperator(
        task_id="create_release_meta_dim",
        sql="sql/create_tables/create_release_meta_dim.sql",
        conn_id="justin_snowflake_conn",
    )
    create_release_styles_bridge = SQLExecuteQueryOperator(
        task_id="create_release_styles_bridge",
        sql="sql/create_tables/create_release_styles_bridge.sql",
        conn_id="justin_snowflake_conn",
    )

    create_styles_dim = SQLExecuteQueryOperator(
        task_id="create_styles_dim",
        sql="sql/create_tables/create_styles_dim.sql",
        conn_id="justin_snowflake_conn",
    )
    create_date_dim = SQLExecuteQueryOperator(
        task_id="create_date_dim",
        sql="sql/create_tables/create_date_dim.sql",
        conn_id="justin_snowflake_conn",
    )

    load_release_meta_dim = SQLExecuteQueryOperator(
        task_id="load_release_meta_dim",
        sql="sql/load_tables/load_release_meta_dim.sql",
        conn_id="justin_snowflake_conn",
    )
    load_release_styles_bridge = SQLExecuteQueryOperator(
        task_id="load_release_styles_bridge",
        sql="sql/load_tables/load_release_styles_bridge.sql",
        conn_id="justin_snowflake_conn",
    )

    load_styles_dim = SQLExecuteQueryOperator(
        task_id="load_styles_dim",
        sql="sql/load_tables/load_styles_dim.sql",
        conn_id="justin_snowflake_conn",
    )

    create_release_meta_dim >> load_release_meta_dim
    create_release_styles_bridge >> load_release_styles_bridge
    create_styles_dim >> load_styles_dim
    create_date_dim
