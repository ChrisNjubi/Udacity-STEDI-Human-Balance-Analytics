import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Trusted
Customer_Trusted_node1707644753419 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-klamar/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1707644753419",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1707644757923 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-project-klamar/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1707644757923",
)

# Script generated for node Join
Join_node1707644963446 = Join.apply(
    frame1=Accelerometer_Landing_node1707644757923,
    frame2=Customer_Trusted_node1707644753419,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1707644963446",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1707645032238 = glueContext.getSink(
    path="s3://udacity-project-klamar/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Accelerometer_Trusted_node1707645032238",
)
Accelerometer_Trusted_node1707645032238.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
Accelerometer_Trusted_node1707645032238.setFormat("json")
Accelerometer_Trusted_node1707645032238.writeFrame(Join_node1707644963446)
job.commit()
