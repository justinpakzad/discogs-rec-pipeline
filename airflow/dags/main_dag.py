import datetime as dt
import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator


# Define the DAG using a with statement
with DAG(
    dag_id="discogs_rec_dag",
    start_date=dt.datetime(2024, 11, 20),
    catchup=False,
    schedule_interval="0 12,0 * * *",
    template_searchpath=["/opt/airflow/"],
    # timedelta(hours=10),
    # timedelta(minutes=15),
) as dag:

    user_feedback_generation_first_batch = LambdaInvokeFunctionOperator(
        task_id="lambda_user_feedback_generation_first_batch",
        function_name="discogs-rec-feedback",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="justin_aws_conn",
    )

    user_feedback_generation_second_batch = LambdaInvokeFunctionOperator(
        task_id="lambda_user_feedback_generation_second_batch",
        function_name="discogs-rec-feedback",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="justin_aws_conn",
    )

    user_feedback_transformation = LambdaInvokeFunctionOperator(
        task_id="lambda_user_feedback_transformation",
        function_name="discogs-rec-transformation",
        payload=json.dumps({"lambda": "transformation"}),
        aws_conn_id="justin_aws_conn",
    )

    create_feedback_stg_tmp = SQLExecuteQueryOperator(
        task_id="create_feedback_stg_tmp",
        sql="sql/create_tables/create_feedback_stg_tmp.sql",
        conn_id="justin_snowflake_conn",
    )
    load_feedback_stg_tmp = SQLExecuteQueryOperator(
        task_id="load_feedback_stg_tmp",
        sql="sql/load_tables/load_feedback_stg_tmp.sql",
        conn_id="justin_snowflake_conn",
    )

    create_feedback_fct = SQLExecuteQueryOperator(
        task_id="create_feedback_fct",
        sql="sql/create_tables/create_feedback_fct.sql",
        conn_id="justin_snowflake_conn",
    )

    create_user_dim = SQLExecuteQueryOperator(
        task_id="create_user_dim",
        sql="sql/create_tables/create_user_dim.sql",
        conn_id="justin_snowflake_conn",
    )
    insert_into_feedback_fct = SQLExecuteQueryOperator(
        task_id="insert_into_feedback_fct",
        sql="sql/load_tables/insert_into_feedback_fct.sql",
        conn_id="justin_snowflake_conn",
    )
    insert_into_user_dim = SQLExecuteQueryOperator(
        task_id="insert_into_user_dim",
        sql="sql/load_tables/insert_into_user_dim.sql",
        conn_id="justin_snowflake_conn",
    )
    drop_feedback_stg_tmp = SQLExecuteQueryOperator(
        task_id="drop_feedback_stg_tmp",
        sql="DROP TABLE IF EXISTS feedback_stg_tmp",
        conn_id="justin_snowflake_conn",
    )

    # insert into user_dim
    # insert into feedback_fct

    (
        [user_feedback_generation_first_batch, user_feedback_generation_second_batch]
        >> user_feedback_transformation
        >> [create_user_dim, create_feedback_fct, create_feedback_stg_tmp]
        >> load_feedback_stg_tmp
        >> [insert_into_feedback_fct, insert_into_user_dim]
        >> drop_feedback_stg_tmp
    )
