import os


SOURCE_DIR = os.environ.get("SOURCE_DIR", "")
DATA_SUBDIR = os.environ.get("DATA_SUBDIR", "data")
META_SUBDIR = os.environ.get("META_SUBDIR", "meta")

DATA_DIR = os.path.join(SOURCE_DIR, DATA_SUBDIR)
META_DIR = os.path.join(SOURCE_DIR, META_SUBDIR)

# s3 directories
BUCKET_NAME = os.environ.get("BUCKET_NAME", "moj-analytics-lookup-tables")
DATABASE_BASE_DIR = os.environ.get("DATABASE_BASE_DIR", "databases")
RAW_DIR = os.environ.get("RAW_DIR", "raw")
