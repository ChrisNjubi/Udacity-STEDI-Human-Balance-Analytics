import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node step_trainer_landing
step_trainer_landing_node1708519397811 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer", transformation_ctx="step_trainer_landing_node1708519397811")

# Script generated for node customer_trusted
customer_trusted_node1708519522085 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1708519522085")

# Script generated for node SQL Query
SqlQuery190 = '''
select * from s join c on s.serialnumber = c.serialnumber 

'''
SQLQuery_node1710316330214 = sparkSqlQuery(glueContext, query = SqlQuery190, mapping = {"s":step_trainer_landing_node1708519397811, "c":customer_trusted_node1708519522085}, transformation_ctx = "SQLQuery_node1710316330214")

# Script generated for node step-trainer-trusted
steptrainertrusted_node1708957876091 = glueContext.getSink(path="s3://udacity-project-klamar/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1708957876091")
steptrainertrusted_node1708957876091.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1708957876091.setFormat("json")
steptrainertrusted_node1708957876091.writeFrame(SQLQuery_node1710316330214)
job.commit()