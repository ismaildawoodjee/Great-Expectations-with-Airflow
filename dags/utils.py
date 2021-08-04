import os

from airflow.hooks.S3_hook import S3Hook


def local_to_s3(bucket_name, key, file_name, remove_local=False):
    """Loads file from local machine to S3. If file needs to be removed,
    specify `remove_local=True`.

    Args:
        bucket_name (str): S3 bucket name
        key (str): Directory on S3 where file is going to be loaded
        file_name (str): Directory on local system where file is located.
        remove_local (bool, optional): To delete local file. Defaults to False.
    """

    s3_hook = S3Hook()
    s3_hook.load_file(
        filename=file_name, bucket_name=bucket_name, replace=True, key=key
    )
    if remove_local:
        if os.path.isfile(file_name):
            os.remove(file_name)
