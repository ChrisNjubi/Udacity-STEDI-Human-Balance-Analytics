import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1710318430270 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1710318430270")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1710318429232 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1710318429232")

# Script generated for node Join
step_trainer_trusted_node1710318430270DF = step_trainer_trusted_node1710318430270.toDF()
accelerometer_trusted_node1710318429232DF = accelerometer_trusted_node1710318429232.toDF()
Join_node1710339587402 = DynamicFrame.fromDF(step_trainer_trusted_node1710318430270DF.join(accelerometer_trusted_node1710318429232DF, (step_trainer_trusted_node1710318430270DF['sensorreadingtime'] == accelerometer_trusted_node1710318429232DF['timestamp']), "right"), glueContext, "Join_node1710339587402")

# Script generated for node Drop Fields
DropFields_node1710339658687 = DropFields.apply(frame=Join_node1710339587402, paths=[], transformation_ctx="DropFields_node1710339658687")

# Script generated for node machine_learning_curated
machine_learning_curated_node1710320134209 = glueContext.getSink(path="s3://udacity-project-klamar/machine_learning/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1710320134209")
machine_learning_curated_node1710320134209.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1710320134209.setFormat("json")
machine_learning_curated_node1710320134209.writeFrame(DropFields_node1710339658687)
job.commit()