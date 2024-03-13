import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1707646114751 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-klamar/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Trusted_node1707646114751",
)

# Script generated for node Drop Fields
DropFields_node1707646550464 = DropFields.apply(
    frame=Accelerometer_Trusted_node1707646114751,
    paths=["z", "y", "x", "user", "timestamp"],
    transformation_ctx="DropFields_node1707646550464",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1708518017764 = DynamicFrame.fromDF(
    DropFields_node1707646550464.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1708518017764",
)

# Script generated for node customer_curated
customer_curated_node1707646643468 = glueContext.getSink(
    path="s3://udacity-project-klamar/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1707646643468",
)
customer_curated_node1707646643468.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
customer_curated_node1707646643468.setFormat("json")
customer_curated_node1707646643468.writeFrame(DropDuplicates_node1708518017764)
job.commit()
