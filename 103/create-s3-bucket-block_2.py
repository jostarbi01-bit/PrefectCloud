from time import sleep
from importlib import import_module

try:    prefect_aws = import_module("prefect_aws")
except ModuleNotFoundError as exc:
    if exc.name != "prefect_aws":
        raise
    raise ModuleNotFoundError(
        "The Prefect AWS integration is missing. Install it with: pip install prefect-aws"
    ) from exc

S3Bucket = prefect_aws.S3Bucket
AwsCredentials = prefect_aws.AwsCredentials

def create_aws_creds_block():
    # environment variables can be helpful for creating credentials blocks
    # do not store credential values in public locations (e.g. GitHub public repo)
    my_aws_creds_obj = AwsCredentials(
        aws_access_key_id="123abc",
        aws_secret_access_key="ab123",
    )
    my_aws_creds_obj.save(name="my-aws-creds-block", overwrite=True)

def create_s3_bucket_block():
    aws_creds = AwsCredentials.load("my-aws-creds-block")
    my_s3_bucket_obj = S3Bucket(
        bucket_name="my-first-bucket-abc-123", credentials=aws_creds
    )
    my_s3_bucket_obj.save(name="s3-bucket-block", overwrite=True)

if __name__ == "__main__":
    create_aws_creds_block()
    create_s3_bucket_block() 