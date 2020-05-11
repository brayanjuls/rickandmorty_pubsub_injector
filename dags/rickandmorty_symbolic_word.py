from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.pubsub_operator import (
     PubSubTopicCreateOperator)
from airflow.contrib.operators.dataflow_operator import (
    DataFlowJavaOperator)
from operators import (CleanAndPushEpisodeOperator)
from datetime import datetime, timedelta
from airflow.models import Variable

start_date = datetime(2020, 1, 1)
end_date = datetime(2020, 1, 14)

default_args = {
    'start_date': start_date,
    'end_date': end_date,
    'depends_on_past': False
}

TOPIC_NAME = Variable.get("topic_name")
RICK_TABLE_NAME = Variable.get("rick_table_name")
MORTY_TABLE_NAME = Variable.get("morty_table_name")
DATASET_SOURCE_PATH = Variable.get("dataset_source_path")
GCP_PROJECT_ID = Variable.get("project_id")
BASE_JAR_PATH = Variable.get("base_jar_path")
GCP_CONNECTION_ID = "google_cloud_connection_temp"
with DAG("rickandmorty_symbolic_word", catchup=True, default_args=default_args, schedule_interval='@daily') as dag:
    start_pipeline = DummyOperator(
        task_id="StartPipeline")

    create_pubsub_topic_operator = PubSubTopicCreateOperator(
        task_id="CreatePubSubTopic",
        topic=TOPIC_NAME,
        project=GCP_PROJECT_ID,
        fail_if_exists=False,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    clean_and_push_episode_to_pubsub = CleanAndPushEpisodeOperator(
        task_id="CleanAndPushEpisodeToPubSub",
        topic_name=TOPIC_NAME,
        source_path=DATASET_SOURCE_PATH,
        execution_date='{{ds}}',
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # dialogs_symbolic_word_to_bigquery = DataFlowJavaOperator(
    #     gcp_conn_id=GCP_CONNECTION_ID,
    #     job_name='rickandmorty_symbolic_word',
    #     task_id='ProcessDialogsSymbolicWordToBigQuery',
    #     jar=f'{BASE_JAR_PATH}/rickandmorty_processing_scripts-1.0-SNAPSHOT.jar',
    #     job_class='FilterDialogs',
    #     project=GCP_PROJECT_ID,
    #     options={
    #         'topicName': TOPIC_NAME,
    #         'rickTableName': RICK_TABLE_NAME,
    #         'mortyTableName': MORTY_TABLE_NAME,
    #         'project':GCP_PROJECT_ID
    #     })

    end_pipeline = DummyOperator(task_id="EndPipeline", dag=dag)

    start_pipeline >> create_pubsub_topic_operator >> clean_and_push_episode_to_pubsub
    clean_and_push_episode_to_pubsub >> end_pipeline
