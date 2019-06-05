import os

SOURCE_DIR = os.environ.get("SOURCE_DIR", "")
DATA_SUBDIR = os.environ.get("DATA_SUBDIR", "data")

DATA_DIR = os.path.join(SOURCE_DIR, DATA_SUBDIR)

# s3 directories
BUCKET_NAME = os.environ.get("BUCKET_NAME", "moj-analytics-lookup-tables")
RAW_DIR = os.environ.get("RAW_DIR", "raw")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "lookup_testing_repo")

if os.path.isfile('release/tag'):
    with open('release/tag') as f:
        RELEASE = f.readline()
else:
    RELEASE = "dev"
