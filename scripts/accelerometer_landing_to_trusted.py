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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1696434129259 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1696434129259",
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

# Script generated for node Privacy Filter
PrivacyFilter_node1696368252471 = Join.apply(
    frame1=AccelerometerLanding_node1696434129259,
    frame2=CustomerTrusted_node1696434163725,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyFilter_node1696368252471",
)

# Script generated for node Drop Fields
DropFields_node1696368405404 = DropFields.apply(
    frame=PrivacyFilter_node1696368252471,
    paths=[
        "email",
        "phone",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "birthDay",
        "shareWithPublicAsOfDate",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1696368405404",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696368405404,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erics-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node2",
)

job.commit()
