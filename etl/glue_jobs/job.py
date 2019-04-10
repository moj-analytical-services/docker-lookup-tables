import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'metadata_base_path',
        'database_path',
        'name',
        'raw_path'
    ]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init("{name}_lookup".format(
    name=args['name']
))

df = spark.read.format("csv").option("header", "true").load(args["raw_path"])
df.write.mode('overwrite').format('parquet').save(
    "{database_path}/{name}/".format(
        database_path=args['database_path'],
        name=args['name']
    )
)
