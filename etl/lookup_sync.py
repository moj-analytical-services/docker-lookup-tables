import json
import os

import boto3
from etl_manager.etl import GlueJob
from etl_manager.meta import DatabaseMeta, read_table_json


from logger import get_logger

logger = get_logger(__name__)


def get_meta_json(meta_dir, file_name):
    with open(os.path.join(meta_dir, file_name), "r") as f:
        return json.load(f)


def define_data_meta_path(basepath, table_name, data=True):
    if data:
        name = "data"
        ext = "csv"
    else:
        name = "meta"
        ext = "json"

    path = os.path.join(basepath, table_name, f"{name}.{ext}")
    if not os.path.exists(path):
        path = os.path.join(basepath, table_name, f"{table_name}.{ext}")
        if not os.path.exists(path):
            err_str = f"Could not find file with name {name}.{ext} or {table_name}.{ext} in dir: {os.path.join(basepath, table_name)}"
            raise FileNotFoundError(err_str)

    return path


class LookupTableSync:
    def __init__(self, bucket_name, data_dir, raw_dir, github_repo, release, **kwargs):
        logger.info(f"GITHUB_REPO: {github_repo} | RELEASE: {release}")

        self.s3 = boto3.resource("s3")
        self.data_dir = data_dir
        self.raw_dir = raw_dir
        self.release = release

        self.db_schema = {
            "name": github_repo,
            "bucket": bucket_name,
            "base_folder": f"{github_repo}/database",
            "description": f"A lookup table deployed from {github_repo}",
        }

        if os.path.isfile(os.path.join(self.data_dir, "database_overwrite.json")):
            db_overwrite = get_meta_json(self.data_dir, "database_overwrite.json")
            if "bucket" in db_overwrite:
                valid_prefix = "alpha-lookup-"
                if not db_overwrite.get("bucket", "").startswith(valid_prefix):
                    raise ValueError(f"bucket specified in database_overwrite must start with: {valid_prefix}")
                self.db_schema["bucket"] = db_overwrite.get("bucket")
            if "description" in db_overwrite:
                self.db_schema["description"] = db_overwrite.get("description")

        self.db_name = self.db_schema.get("name")
        self.bucket_name = self.db_schema.get("bucket")
        self.meta_and_files = self.find_meta_and_data_files()

    @property
    def bucket(self):
        """Returns boto3 s3 bucket"""
        return self.s3.Bucket(self.bucket_name)

    @property
    def raw_path(self):
        return f"s3://{self.bucket_name}/{self.raw_key}"

    @property
    def raw_key(self):
        return f"{self.db_name}/{self.raw_dir}/{self.release}"

    @property
    def database_path(self):
        db = DatabaseMeta(**self.db_schema)
        return db.s3_database_path

    def send_to_s3(self, body, key):
        """Sends body as string to s3 bucket with key"""
        logger.info(f"Sending file: {key}")
        self.bucket.put_object(Key=key, Body=body)

    def find_meta_and_data_files(self):
        """get meta and data files"""
        tables = [f.name for f in os.scandir(self.data_dir) if f.is_dir()]
        meta_and_data = {}
        for table_name in tables:
            data_path = define_data_meta_path(self.data_dir, table_name, True)
            meta_path = define_data_meta_path(self.data_dir, table_name, False)
            meta_and_data[table_name] = {
                "meta_path": meta_path,
                "data_path": data_path,
                "raw_data_path": f"{self.raw_path}/data/{data_path}",
                "raw_meta_path": f"{self.raw_path}/meta/{meta_path}",
                "bucket_data_path": f"{self.raw_key}/data/{data_path}",
                "bucket_meta_path": f"{self.raw_key}/meta/{meta_path}",
            }
        return meta_and_data

    def send_raw(self):
        """Send raw files to s3"""
        for name, info in self.meta_and_files.items():
            for k in ["data_path", "meta_path"]:
                with open(info[k]) as f:
                    data = f.read()
                self.send_to_s3(data, info[f"bucket_{k}"])

    def create_glue_database(self):
        """Creates glue database"""
        # Create database based on db_schema
        db = DatabaseMeta(**self.db_schema)
        for name, info in self.meta_and_files.items():
            tm = read_table_json(info["meta_path"], database=db)
            tm.data_format = "parquet"
            # Add a release column as the first file partition to every table
            tm.add_column(
                name="release",
                type="character",
                description="github release tag of this lookup",
            )
            tm.partitions = ["release"] + tm.partitions
            db.add_table(tm)

        db.create_glue_database(delete_if_exists=True)
        db.refresh_all_table_partitions()

    def load_data_to_glue_database(self):
        """Write csv to database bucket as parquet"""
        for name, info in self.meta_and_files.items():
            job = GlueJob(
                job_folder=f"/etl/glue_jobs",
                bucket=self.bucket_name,
                job_role="lookups_job_role",
                job_arguments={
                    "--database_path": self.database_path,
                    "--name": name,
                    "--raw_data_path": info["raw_data_path"],
                    "--raw_meta_path": info["raw_meta_path"],
                    "--release": self.release,
                },
            )
            job.job_name = f"lookup-{self.db_name}-{name}"

            job.run_job()
            logger.info("Job running")
            job.wait_for_completion(verbose=True)
            logger.info("Awaiting completion")

            if job.job_run_state == "SUCCEEDED":
                logger.info("Job successful - tidying")

            job.cleanup()

    def sync(self):
        """Syncs repo with glue dataset"""
        logger.info("Running etl")
        logger.info("Sending raw data:")
        self.send_raw()
        self.load_data_to_glue_database()
        self.create_glue_database()
