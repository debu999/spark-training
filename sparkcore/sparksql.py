from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, avro
from pyspark.sql.avro.functions import from_avro, to_avro
from pprint import pp
from time import sleep
import sys

from domains import Employee
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    DoubleType,
)

# conf = SparkConf().setAppName("deb-spark").setMaster("local[*]")
# sc = SparkContext(master="local", appName="deb-spark")
sc = SparkContext(appName="deb-spark")
spark = (
    SparkSession.builder.appName("deb-spark-sql")
    .master("local[*]")
    .config(conf=SparkConf())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile("./sparkcore/domains.py")
sc._jsc.hadoopConfiguration().set(
    "mapreduce.input.fileinputformat.input.dir.recursive", "true"
)

employee_schema = StructType(
    [
        StructField("empId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("designation", StringType(), True),
    ]
)


emp_schema = StructType(
    [
        StructField("empId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("designation", StringType(), True),
        StructField("salary", DoubleType(), True),
    ]
)


def query_json():
    df = spark.read.option("multiLine", True).json(
        "c:/spark-training/samples/sample.json"
    )
    pp(df.toPandas().to_string())
    df.show()
    df.printSchema()
    df.write.parquet("c:/spark-training/outputs/o1.parquet")


def read_from_parquet():
    df = spark.read.parquet(
        "c:/spark-training/outputs/o1.parquet/part-00000-9e90f5d4-023f-490d-ba9d-a005bd42cf1e-c000.snappy.parquet"
    )
    df.show()


def write_to_avro():
    df = spark.createDataFrame(
        [(1, "John", 25), (2, "Mary", 31), (3, "David", 42)], ["id", "name", "age"]
    )

    # write the DataFrame to an Avro file
    df.write.format("avro").save("c:/spark-training/outputs/output.avro")


def read_from_avro():
    df = spark.read.format("avro").load("c:/spark-training/outputs/output.avro")
    df.show()


def read_from_mysql_db():
    # spark._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/trainingdb")
        .option("user", "root")
        .option("password", "rps@12345")
        .option("dbtable", "employee")
        .load()
    )
    df.show()


def csv_to_db():
    emp_df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(employee_schema)
        .load("c:/spark-training/samples/employee.csv")
    )
    emp_df.show()
    emp_df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(employee_schema)
        .load("c:/spark-training/samples/employee_malformed.csv")
    )
    emp_df.show()

    emp_df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(employee_schema)
        .option("mode", "DROPMALFORMED")
        .load("c:/spark-training/samples/employee_malformed.csv")
    )
    emp_df.show()

    try:
        emp_df = (
            spark.read.format("csv")
            .option("header", True)
            .schema(employee_schema)
            .option("mode", "FAILFAST")
            .load("c:/spark-training/samples/employee_malformed.csv")
        )
        emp_df.show()
    except Exception as e:
        pp(e)


def groupby_agg():
    emp_df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(employee_schema)
        .load("c:/spark-training/samples/employee.csv")
    )
    grouped_set = emp_df.groupBy("designation")
    count_df = grouped_set.count()
    count_df.show()
    emp_df.groupBy("designation").count().show()

    emp_df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("c:/spark-training/samples/employees.csv")
    )
    emp_df.show()
    emp_df.groupBy("designation").count().show()
    emp_df.groupBy("designation").max("salary").show()
    emp_df.groupBy("designation").sum("salary").show()
    emp_df.groupBy("designation").min("salary").show()
    emp_df.printSchema()
    emp_df.groupBy("designation").agg(sum("salary").alias("total_salary")).show()


def joins_concepts():
    people_df = (
        spark.read.format("csv")
        .option("header", True)
        .load("c:/spark-training/samples/people.csv")
    )
    postalcode_df = (
        spark.read.format("csv")
        .option("header", True)
        .load("c:/spark-training/samples/postalcodes.csv")
    )
    postalcode_df.show()
    people_df.show()

    join_df = people_df.join(postalcode_df, on="pcode")
    join_df.show()
    join_df.drop(postalcode_df.state).show()
    emp_df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("c:/spark-training/samples/employees.csv")
    )
    address_df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("c:/spark-training/samples/address.csv")
    )
    emp_df.show
    address_df.show
    emp_df.join(address_df, emp_df.empId == address_df.id).show()
    emp_df.join(address_df, emp_df.empId == address_df.id).drop(address_df.id).show()

    emp_df.createTempView("employee")
    spark.sql("select * from employee").show()
    spark.sql("select empId,name from employee").show()
    people_df.createTempView("people")
    postalcode_df.createTempView("postalcode")
    people_df.show()
    spark.sql(
        "select a.pcode,a.lastName,a.firstName,b.city,b.state from people a,postalcode b where a.pcode=b.pcode"
    ).show()
    people_df.join(
        postalcode_df, people_df.pcode == postalcode_df.pcode, "left_outer"
    ).show()
    people_df.join(
        postalcode_df, people_df.pcode == postalcode_df.pcode, "right_outer"
    ).show()
    people_df.join(
        postalcode_df, people_df.pcode == postalcode_df.pcode, "outer"
    ).show()


def to_employee_objects():
    # Read the CSV file into a Spark DataFrame
    df = spark.read.csv(
        "c:/spark-training/samples/employees.csv", schema=emp_schema, header=True
    )
    df.show()
    # Convert the DataFrame into a list of Employee objects
    emps = df.rdd.map(
        lambda row: Employee(row.empId, row.name, row.designation, row.salary)
    ).collect()
    # Print the list of Employee objects
    for employee in emps:
        print(employee.__dict__)


if __name__ == "__main__":
    # query_json()
    # read_from_parquet()
    # write_to_avro()
    # read_from_avro()
    # read_from_mysql_db()
    # csv_to_db()
    # groupby_agg()
    # joins()
    # joins_concepts()
    to_employee_objects()
    sc.stop()
    spark.stop()
