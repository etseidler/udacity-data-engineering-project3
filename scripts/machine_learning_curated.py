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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696516464488 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1696516464488",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1696516491391 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1696516491391",
)

# Script generated for node Step Trainer to Accelerometer
StepTrainertoAccelerometer_node1696516566976 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1696516464488,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="StepTrainertoAccelerometer_node1696516566976",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696516660775 = ApplyMapping.apply(
    frame=CustomerTrusted_node1696516491391,
    mappings=[
        ("serialNumber", "string", "customer_data_serialNumber", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696516660775",
)

# Script generated for node Step Trainer + Accelerometer to Customer
StepTrainerAccelerometertoCustomer_node1696516628893 = Join.apply(
    frame1=StepTrainertoAccelerometer_node1696516566976,
    frame2=RenamedkeysforJoin_node1696516660775,
    keys1=["serialNumber"],
    keys2=["customer_data_serialNumber"],
    transformation_ctx="StepTrainerAccelerometertoCustomer_node1696516628893",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=StepTrainerAccelerometertoCustomer_node1696516628893,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erics-stedi-lakehouse/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node2",
)

job.commit()
