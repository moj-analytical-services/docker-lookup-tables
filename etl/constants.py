import os

SOURCE_DIR = os.environ.get("SOURCE_DIR", "")
DATA_SUBDIR = os.environ.get("DATA_SUBDIR", "data")

DATA_DIR = os.path.join(SOURCE_DIR, DATA_SUBDIR)

# s3 directories
BUCKET_NAME = os.environ.get("BUCKET_NAME", "moj-analytics-lookup-tables")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "lookup_testing_repo")

if os.environ.get("RELEASE_TAG"):
    RELEASE = os.environ.get("RELEASE_TAG")
elif os.path.isfile("release/tag"):
    with open("release/tag") as f:
        RELEASE = f.readline()
else:
    RELEASE = "dev"

REGION = "eu-west-1"
