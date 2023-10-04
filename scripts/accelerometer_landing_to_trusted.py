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
AccelerometerLanding_node1696368640588 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1696368640588",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1696367716420 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1696367716420",
)

# Script generated for node Join Customer
JoinCustomer_node1696368252471 = Join.apply(
    frame1=CustomerTrustedZone_node1696367716420,
    frame2=AccelerometerLanding_node1696368640588,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1696368252471",
)

# Script generated for node Drop Fields
DropFields_node1696368405404 = DropFields.apply(
    frame=JoinCustomer_node1696368252471,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
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
