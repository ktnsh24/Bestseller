import logging
import os
import boto3

from configuration import read_config


logger = logging.getLogger(__name__)
config = read_config()
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(config['bucket_name'])
prefix = f"{config['bucket_source_folder']}" + "/"


def download_s3_data():
    """
    Retrieve data from S3 bucket.
    """
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key == prefix:
            os.makedirs(os.path.dirname(obj.key), exist_ok=True)
            continue;
        bucket.download_file(obj.key, obj.key)
        logger.info(f"File {obj.key} downloaded")
    return


def move_object() -> None:
    """
    Move an object in S3 bucket.
    """
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key == prefix:
            continue;
        s3_resource.meta.client.copy({
                        'Bucket': config['bucket_name'],
                        'Key': obj.key},
                        Key=config['bucket_archive_folder'] + '/' + obj.key,
                        Bucket=config['bucket_name'])
        logger.info(f"File {obj.key} is moved to {config['bucket_archive_folder']} folder")
    delete_object()
    return


def delete_object() -> None:
    """
    Delete an object in S3 bucket.
    """
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key == prefix:
            continue;
        s3_resource.Object(config['bucket_name'], obj.key).delete()
    return
