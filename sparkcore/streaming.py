from pprint import pp
from time import sleep
from typing import Dict
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, avro
from pyspark import SparkConf, SparkContext

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    DoubleType,
)

spark = (
    SparkSession.builder.appName("deb-spark-streaming").master("local[*]").getOrCreate()
)
emp_schema = StructType(
    [
        StructField("empId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("designation", StringType(), True),
        StructField("salary", DoubleType(), True),
    ]
)

input_datastream = (
    spark.readStream.format("csv")
    .option("header", True)
    .schema(emp_schema)
    .load("c:/spark-training/streaming/input")
)

developer_datastream = input_datastream.filter(
    input_datastream.designation == "Developer"
)
accountant_datastream = input_datastream.filter(
    input_datastream.designation == "Accountant"
)
others_datastream = input_datastream.filter(
    ~input_datastream.designation.isin(["Developer", "Accountant"])
)

# Define the output directories based on designation
output_dirs: Dict = {
    "Developer": "c:/spark-training/streaming/input/developer",
    "Accountant": "c:/spark-training/streaming/input/accountant",
    "Architect": "c:/spark-training/streaming/input/architect",
}

# Define a StreamingQuery
d_query = (
    developer_datastream.writeStream.trigger(processingTime="10 seconds")
    .format("json")
    .option("checkpointLocation", "c:/spark-training/streaming/checkpoint/developer")
    .start(output_dirs.get("Developer"))
)

a_query = (
    accountant_datastream.writeStream.trigger(processingTime="10 seconds")
    .format("json")
    .option("checkpointLocation", "c:/spark-training/streaming/checkpoint/accountant")
    .start(output_dirs.get("Accountant"))
)

o_query = (
    others_datastream.writeStream.trigger(processingTime="10 seconds")
    .format("json")
    .option("checkpointLocation", "c:/spark-training/streaming/checkpoint/others")
    .start("c:/spark-training/streaming/input/others")
)

sleep(5000)
pp("streaming stopped")
d_query.stop()
a_query.stop()
o_query.stop()
