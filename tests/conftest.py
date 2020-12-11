import os
import shutil
import tempfile
import json

import pytest
from moto import mock_s3
import boto3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def setup_env_and_s3(aws_credentials):
    os.environ["GITHUB_REPO"] = "dummy_repo"
    os.environ["BUCKET_NAME"] = "test-bucket"

    old_cwd = os.getcwd()

    # Make a temporary directory and repo structure to it
    newdir = tempfile.mkdtemp(dir="")
    data_dir = os.path.join(newdir, os.environ["GITHUB_REPO"], "data")
    os.makedirs(data_dir)

    os.mkdir(os.path.join(data_dir, "lookup1"))
    os.mkdir(os.path.join(data_dir, "lookup2"))

    # Write files from test/data into a new folder structure
    # Read in test data
    with open("tests/data/data.csv") as f:
        lu_data = "".join(f.readlines())

    with open("tests/data/meta.json") as f:
        lu_meta = json.load(f)

    # Write data and meta to lookup1 and lookup2
    for lu_name in ["lookup1", "lookup2"]:
        dataname = "data.csv" if lu_name == "lookup1" else f"{lu_name}.csv"
        metaname = "meta.json" if lu_name == "lookup1" else f"{lu_name}.json"
        with open(os.path.join(data_dir, lu_name, dataname), "w") as f:
            f.write(lu_data)

        lu_meta["name"] = f"{lu_name}"
        lu_meta["location"] = f"{lu_name}/"
        with open(os.path.join(data_dir, lu_name, metaname), "w") as f:
            json.dump(lu_meta, f)

    # Set as base dir to mimic how this would run (basedir or repo)
    fake_repo_dir = os.path.join(newdir, os.environ["GITHUB_REPO"])
    os.chdir(fake_repo_dir)

    # Mock S3 and yield response
    with mock_s3():
        s3_resource = boto3.resource("s3", region_name="eu-west-1")
        for b in ["alpha-lookup-overwrite-bucket", "moj-analytics-lookup-tables"]:
            s3_resource.meta.client.create_bucket(
                Bucket=b,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
            )
        yield s3_resource

    # Reset envs and delete tmp dir on closedown
    os.chdir(old_cwd)
    shutil.rmtree(newdir)
    del os.environ["GITHUB_REPO"]
    del os.environ["BUCKET_NAME"]
