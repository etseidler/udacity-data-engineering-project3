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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1696434129259 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1696434129259",
)

# Script generated for node Customer Curated
CustomerCurated_node1696434163725 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erics-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1696434163725",
)

# Script generated for node Renamed keys for Privacy Filter
RenamedkeysforPrivacyFilter_node1696514037535 = ApplyMapping.apply(
    frame=CustomerCurated_node1696434163725,
    mappings=[("serialNumber", "string", "right_serialNumber", "string")],
    transformation_ctx="RenamedkeysforPrivacyFilter_node1696514037535",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1696368252471 = Join.apply(
    frame1=StepTrainerLanding_node1696434129259,
    frame2=RenamedkeysforPrivacyFilter_node1696514037535,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="PrivacyFilter_node1696368252471",
)

# Script generated for node Drop Fields
DropFields_node1696368405404 = DropFields.apply(
    frame=PrivacyFilter_node1696368252471,
    paths=["right_serialNumber"],
    transformation_ctx="DropFields_node1696368405404",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696514705650 = DynamicFrame.fromDF(
    DropFields_node1696368405404.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696514705650",
)

# Script generated for node Step Landing Trusted
StepLandingTrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696514705650,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erics-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepLandingTrusted_node2",
)

job.commit()
