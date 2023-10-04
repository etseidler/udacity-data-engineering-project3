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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696434129259 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1696434129259",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1696434163725 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1696434163725",
)

# Script generated for node Has Accelerometer Data
HasAccelerometerData_node1696368252471 = Join.apply(
    frame1=AccelerometerTrusted_node1696434129259,
    frame2=CustomerTrusted_node1696434163725,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="HasAccelerometerData_node1696368252471",
)

# Script generated for node Drop Fields
DropFields_node1696368405404 = DropFields.apply(
    frame=HasAccelerometerData_node1696368252471,
    paths=["y", "z", "timeStamp", "user", "x"],
    transformation_ctx="DropFields_node1696368405404",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696437922532 = DynamicFrame.fromDF(
    DropFields_node1696368405404.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696437922532",
)

# Script generated for node Customer Curated
CustomerCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696437922532,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erics-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node2",
)

job.commit()
