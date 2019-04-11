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


class LookupTableSync:
    def __init__(self, bucket_name, meta_dir, data_dir, raw_dir, github_repo, release, database_base_dir, **kwargs):
        
        logger.info(f"RELEASE: {release}| GITHUB_REPO: {github_repo}")
        github_repo = github_repo[len("lookup_"):]
        self.s3 = boto3.resource("s3")
        self.meta_dir = os.path.join(release, meta_dir)
        self.data_dir = data_dir
        self.raw_dir = raw_dir
        self.release = release
        
        if os.path.isfile(os.path.join(self.meta_dir, "database.json")):
            self.db_schema = get_meta_json(self.meta_dir, "database.json")
        else:
            self.db_schema = {
                "name": github_repo,
                "bucket": bucket_name,
                "base_folder": database_base_dir,
                "description": f"A lookup table deployed from {github_repo}"
            }

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
        return f"{self.raw_dir}/{self.release}/{self.db_name}"

    @property
    def database_path(self):
        return f"s3://{self.bucket_name}/database/{self.db_name}"

    def send_to_s3(self, body, key):
        """Sends body as string to s3 bucket with key"""
        logger.info(f"Sending file: {key}")
        self.bucket.put_object(Key=key, Body=body)

    def find_meta_and_data_files(self):
        """get meta and data files"""
        metas = os.listdir(self.meta_dir)
        data = os.listdir(self.data_dir)

        meta_and_data = {}
        for data_path in data:
            name = data_path[:data_path.rfind(".")]
            meta_path = next((m for m in metas if m.startswith(name)), None)
            meta_and_data[name] = {
                "meta_path": os.path.join(self.meta_dir, meta_path),
                "data_path": os.path.join(self.data_dir, data_path),
                "raw_path": f"{self.raw_path}/{data_path}",
                "bucket_path": f"{self.raw_key}/{data_path}"
            }
        return meta_and_data

    def send_raw(self):
        """Send raw files to s3"""
        for name, info in self.meta_and_files.items():
            with open(info["data_path"]) as f:
                data = f.read()
            self.send_to_s3(data, info["bucket_path"])

    def create_glue_database(self):
        """Creates glue database"""
        # Create database based on db_schema
        db = DatabaseMeta(**self.db_schema)

        files = os.listdir(self.meta_dir)
        files = [f for f in files if f.endswith('.json') and f != "database.json"]

        for f in files:
            table_file_path = os.path.join(self.meta_dir, f)
            tm = read_table_json(table_file_path, database=db)
            # Add a release column as the first file partition to every table
            tm.add_column(name="release", type="character", "github release tag of this lookup")
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
                    '--database_path': self.database_path,
                    '--name': name,
                    '--raw_path': info["raw_path"]
                }
            )
            job.job_name = f"lookup-{self.db_name}-{name}"

            job.run_job()
            logger.info("Job running")
            job.wait_for_completion(verbose=True)
            logger.info("Awaiting completion")

            if job.job_run_state == 'SUCCEEDED':
                logger.info("Job successful - tidying")

            job.cleanup()

    def sync(self):
        """Syncs repo with glue dataset"""
        logger.info("Running etl")
        logger.info("Sending raw data:")
        self.send_raw()
        self.create_glue_database()
        self.load_data_to_glue_database()
