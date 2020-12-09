import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from gluejobutils.datatypes import align_df_to_meta
from gluejobutils.s3 import read_json_from_s3

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "metadata_base_path",
        "database_path",
        "name",
        "raw_data_path",
        "raw_meta_path",
        "release",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init("{name}_lookup".format(name=args["name"]))

df = spark.read.format("csv").option("header", "true").load(args["raw_data_path"])
meta = read_json_from_s3(args["raw_meta_path"])

df = align_df_to_meta(df, meta)

df.write.mode("overwrite").format("parquet").save(
    "{database_path}/{name}/release={release}/".format(
        database_path=args["database_path"], name=args["name"], release=args["release"]
    )
)
