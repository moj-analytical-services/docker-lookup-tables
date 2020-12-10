import os
import pytest

from moto import mock_s3
import boto3


@pytest.fixture(scope="function")
def setup_env():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["SOURCE_DIR"] = "tests/dummy_repo/"
    os.environ["GITHUB_REPO"] = "dummy_repo"
    os.environ["BUCKET_NAME"] = "test-bucket"


@pytest.fixture(scope="function")
def s3(setup_env):
    with mock_s3():
        b = os.environ["BUCKET_NAME"]
        s3_resource = boto3.resource("s3", region_name="eu-west-1")
        s3_resource.meta.client.create_bucket(
            Bucket=b,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        yield s3_resource


@pytest.fixture(scope="function")
def s3_client(setup_env):
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-1")
