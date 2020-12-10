import json
import os

import boto3
from etl_manager.meta import DatabaseMeta, read_table_json
from etl.constants import REGION
import pyarrow as pa
from pyarrow import fs, csv, parquet as pq

from etl.logger import get_logger

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
            err_str = (
                f"Could not find file with name {name}.{ext} or {table_name}.{ext} "
                f"in dir: {os.path.join(basepath, table_name)}"
            )
            raise FileNotFoundError(err_str)

    return path


def read_csv_write_to_parquet(local_data_path, s3_path, local_meta_path):

    local = fs.LocalFileSystem()
    s3 = fs.S3FileSystem(region=REGION)
    with local.open_input_stream(local_data_path) as f:
        tab = csv.read_csv(f)

    metadata = read_table_json(local_meta_path)
    arrow_cols = []
    for col in metadata.columns:
        if col["name"] not in metadata.partitions:
            arrow_cols.append(convert_meta_col_to_arrow_tuple(col))

    s = pa.schema(arrow_cols)
    tab = tab.cast(s)

    with s3.open_input_stream(s3_path) as f:
        pq.write_table(tab, f)


def convert_meta_col_to_arrow_tuple(col):

    schema_convert = {
        "character": pa.string,
        "int": pa.int64,
        "long": pa.int64,
        "float": pa.float64,
        "double": pa.float64,
        "date": pa.date64,
        "boolean": pa.bool_,
        "binary": pa.binary,
        "decimal": None,
    }

    if col["type"].startswith("decimal"):
        a, b = col["type"].split("(", 1)[1].split(")", 1)[0].split(",", 1)
        t = (col["name"], pa.decimal128(int(a), int(b)))
    elif col["type"] == "datetime":
        t = (col["name"], pa.timestamp("s"))
    else:
        t = (col["name"], schema_convert[col["type"]]())

    return t


class LookupTableSync:
    def __init__(
        self,
        bucket_name,
        source_dir,
        data_dir,
        github_repo,
        release,
        **kwargs
    ):
        logger.info(f"GITHUB_REPO: {github_repo} | RELEASE: {release}")

        self.s3 = boto3.resource("s3")
        self.source_dir = source_dir
        self.data_dir = data_dir
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
                    raise ValueError(
                        "bucket specified in database_overwrite "
                        f"must start with: {valid_prefix}"
                    )
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
        return f"{self.db_name}/{self.release}"

    @property
    def database_path(self):
        db = DatabaseMeta(**self.db_schema)
        return db.s3_database_path

    def send_to_s3(self, body, key):
        """Sends body as string to s3 bucket with key"""
        logger.info(f"Sending file: {key}")
        self.bucket.put_object(Key=key, Body=body)

    def find_meta_and_data_files(self):
        """
        Get meta and data files.

        Each folder in the base repo represents a table (name of folder = table name)

        Returns a dictionary with each key as the table_name. Each value is:
        {
            "meta_path": Path to the tables metadata JSON locally (in the git repo),
            "data_path": Path to the tables data CSV locally (in the git repo),
            "data_obj_key": S3 object path to where the exact copy of the local data
                CSV file will be uploaded to,
            "meta_obj_key": S3 object path to where the exact copy of the local
                metadata JSON file will be uploaded to,
        }
        """
        tables = [f.name for f in os.scandir(self.data_dir) if f.is_dir()]
        meta_and_data = {}
        for table_name in tables:
            data_path = define_data_meta_path(self.data_dir, table_name, True)
            meta_path = define_data_meta_path(self.data_dir, table_name, False)

            # For concourse
            data_s3_path = data_path.replace("lookup-source/", "", 1)
            meta_s3_path = meta_path.replace("lookup-source/", "", 1)

            # In case not uploading lookup from root dir (e.g. source_dir != "")
            if self.source_dir:
                data_s3_path = data_path.replace(self.source_dir, "", 1)
                meta_s3_path = meta_path.replace(self.source_dir, "", 1)

            meta_and_data[table_name] = {
                "meta_path": meta_path,
                "data_path": data_path,
                "data_obj_key": f"{self.raw_key}/{data_s3_path}",
                "meta_obj_key": f"{self.raw_key}/{meta_s3_path}",
            }
        return meta_and_data

    def send_raw(self):
        """Send local files to S3 in their raw form"""
        for table_name, data_paths in self.meta_and_files.items():
            for k in ["data_path", "meta_path"]:
                prefix = "data" if k == "data_path" else "meta"
                with open(data_paths[k]) as f:
                    data = f.read()
                self.send_to_s3(data, data_paths[f"{prefix}_obj_key"])

    def create_glue_database(self):
        """Creates glue database"""
        # Create database based on db_schema
        db = DatabaseMeta(**self.db_schema)
        for table_name, data_paths in self.meta_and_files.items():
            tm = read_table_json(data_paths["meta_path"], database=db)
            tm.data_format = "parquet"
            if tm.partitions:
                raise AttributeError(
                    "Automated lookup tables can only be "
                    "partitioned by their GitHub release"
                )
            # Add a release column as the first file partition to every table
            tm.add_column(
                name="release",
                type="character",
                description="github release tag of this lookup",
            )
            tm.partitions = ["release"]
            db.add_table(tm)

        db.create_glue_database(delete_if_exists=True)
        db.refresh_all_table_partitions()

    def load_data_to_glue_database(self):
        """Write csv to database bucket as parquet"""
        for table_name, data_paths in self.meta_and_files.items():
            out_path = os.path.join(
                self.database_path,
                table_name,
                f"release={self.release}",
                f"{table_name}_{self.release}.parquet.snappy",
            )
            read_csv_write_to_parquet(
                data_paths["data_path"], out_path, data_paths["meta_path"]
            )

    def sync(self):
        """Syncs repo with glue dataset"""
        logger.info("Running etl")
        logger.info("Sending raw data:")
        self.send_raw()
        self.load_data_to_glue_database()
        self.create_glue_database()
