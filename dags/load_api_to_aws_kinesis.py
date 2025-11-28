from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests

## External package
import json
from datetime import datetime
import boto3
import logging

logger = logging.getLogger(__name__)

api_base_url = 'https://jsonplaceholder.typicode.com'
kinesis_client = boto3.client('kinesis')   

## Setting up incremental user id for next call
def _set_api_user_id(api_user_id, **kwargs):
    try:
        logger.info(f"type:: {type(api_user_id)} and api_user_id:: {api_user_id}")

        if api_user_id == -1 or api_user_id == 10:
            Variable.set(key="api_user_id", value=1)
        else:
            Variable.set(key="api_user_id", value=int(api_user_id)+1)
        return f"Latest api user id {Variable.get('api_user_id')} set successfully"

    except Exception as e:
        logger.error(f"ERROR WHILE SETTING UP userId param value:: {e}")
        raise

def _extract_userposts(new_api_user_id=1, **kwargs):
    try:
        ti = kwargs["ti"]
        logger.info(f"type:: {type(new_api_user_id)} and new_api_user_id:: {new_api_user_id}")
        response = requests.get(f"{api_base_url}/posts?userId={int(new_api_user_id)}")
        user_posts = response.json()
        logger.info(f"api data || user_posts:: {user_posts}")

        # PUSH TO XCOM
        ti.xcom_push(key='user_posts', value=user_posts)

        return user_posts

    except Exception as e:
        logger.error(f"ERROR WHILE FETCHING USER POSTS API DATA:: {e}")
        raise

def _process_user_posts(new_api_user_id=1, **kwargs):
    try:
        ti = kwargs["ti"]
        stream_name = "user-posts-data-stream"

        # PULL FROM XCOM
        user_posts = ti.xcom_pull(task_ids='extract_userposts', key='user_posts')

        logger.info(f"api data || user_posts:: {user_posts}")

        for user_post in user_posts:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(user_post) + '\n',
                PartitionKey=str(user_post['userId']),
                SequenceNumberForOrdering=str(user_post['id'] - 1)
            )

            logger.info(
                f"Produced Kinesis record {response['SequenceNumber']} "
                f"to Shard {response['ShardId']} "
                f"status {response['ResponseMetadata']['HTTPStatusCode']}"
            )

        return f"Total {len(user_posts)} posts written to Kinesis stream `{stream_name}`"

    except Exception as e:
        logger.error(f"ERROR WHILE WRITING USER POSTS TO KINESIS STREAM:: {e}")
        raise


with DAG(
    dag_id='load_api_aws_kinesis',
    default_args={'owner': 'Sovan'},
    tags=["api data load to s3"],
    start_date=datetime(2023, 9, 24),
    schedule='@daily',
    catchup=False
):

    get_api_userId_params = PythonOperator(
        task_id='get_api_userId_params',
        python_callable=_set_api_user_id,
        op_args=[int(Variable.get("api_user_id", default_var=-1))]
    )

    extract_userposts = PythonOperator(
        task_id='extract_userposts',
        python_callable=_extract_userposts,
        op_kwargs={"new_api_user_id": int(Variable.get("api_user_id", default_var=-1))}
    )

    write_userposts_to_stream = PythonOperator(
        task_id='write_userposts_to_stream',
        python_callable=_process_user_posts,
        op_kwargs={"new_api_user_id": int(Variable.get("api_user_id", default_var=-1))}
    )

    get_api_userId_params >> extract_userposts >> write_userposts_to_stream




