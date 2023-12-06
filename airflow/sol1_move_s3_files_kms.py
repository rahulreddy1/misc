from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.transfers.s3_to_s3 import S3ToS3Operator

default_args = {
    'owner': 'your_owner',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_file_transfer',
    default_args=default_args,
    description='A simple DAG to transfer files between S3 buckets',
    schedule_interval=timedelta(days=1),  # You can adjust the schedule as needed
)

source_bucket = 'your_source_bucket'
destination_bucket = 'your_destination_bucket'
source_key = 'path/to/source/file.txt'
destination_key = 'path/to/destination/file.txt'
aws_conn_id = 'your_aws_connection_id'  # Ensure you have an AWS connection set up in Airflow

transfer_task = S3ToS3Operator(
    task_id='transfer_file',
    source_bucket_key=source_key,
    dest_bucket_key=destination_key,
    source_bucket_name=source_bucket,
    dest_bucket_name=destination_bucket,
    aws_conn_id=aws_conn_id,
    replace=True,  # Set to True if you want to overwrite the destination file if it already exists
    encryption_options={
        'SSEKMSKeyId': 'your_kms_key_id',  # Replace with your KMS key ID
        'CopySourceServerSideEncryption': 'aws:kms',  # Enable server-side encryption during the copy operation
    },
    dag=dag,
)

# You can define more tasks or dependencies as needed

if __name__ == "__main__":
    dag.cli()
