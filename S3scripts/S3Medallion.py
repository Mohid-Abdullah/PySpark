import os
from turtle import rt
import warnings
import json
import copy
warnings.filterwarnings("ignore")

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars jars/hadoop-aws-3.3.1.jar,jars/aws-java-sdk-bundle-1.11.1026.jar pyspark-shell"

from pyspark.sql import DataFrameNaFunctions, SparkSession
from pyspark.sql.functions import col, lit, lower, when, to_date, to_timestamp, date_format, row_number, concat_ws, split, initcap
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pyspark.sql.window import Window

#Mimicking S3 buckets using local Dockerized MinIO system
spark = (SparkSession.builder
        .appName("PySpark with MinIO")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("FATAL")

#configuration settings
with open("S3config.json", "r") as f:
    config = json.load(f)
rtb_config = copy.deepcopy(config)

#reading from raw
emp_csv = spark.read.options(**rtb_config["options"]).csv(rtb_config["EmpCsvReadAddress"])
dept_csv = spark.read.options(**rtb_config["options"]).csv(rtb_config["DepCsvReadAddress"])
emp_json = spark.read.options(**rtb_config["options"]).json(rtb_config["EmpJsonReadAddress"])

#raw to bronze
emp_json = emp_json.withColumnRenamed("Department_Name","Department")
emp_full = emp_csv.unionByName(emp_json)
emp_full.write.format(rtb_config["format"]).options(**rtb_config["options"]).mode(rtb_config["mode"]).save(rtb_config["EmpWriteAddress"])
dept_csv.write.format(rtb_config["format"]).options(**rtb_config["options"]).mode(rtb_config["mode"]).save(rtb_config["DepWriteAddress"])

#processing bronze to silver
emp_bronze = spark.read.options(**rtb_config["options2"]).format(rtb_config["format"]).load(rtb_config["EmpWriteAddress"])
dept_bronze = spark.read.options(**rtb_config["options2"]).format(rtb_config["format"]).load(rtb_config["DepWriteAddress"])

# Cleaning Salary
emp_bronze = emp_bronze.withColumn(
    "Salary",
    when(
        (lower(col("Salary")) == "n/a") |
        (lower(col("Salary")) == "null") |
        (col("Salary").isNull()) |
        (col("Salary") == "") |
        (lower(col("Salary")) == "n/a"),
        "25000"
    ).otherwise(col("Salary"))
).withColumn("Salary", col("Salary").cast("float"))

# cleaning name and department
emp_bronze = emp_bronze.filter(
    (col("Name").isNotNull()) &
    (col("Name") != "") &
    (lower(col("Name")) != "null") &
    (lower(col("Name")) != "n/a") &
    (col("Name") != "-") &
    (col("Department").isNotNull()) &
    (col("Department") != "") &
    (lower(col("Department")) != "null") &
    (lower(col("Department")) != "n/a") &
    (col("Department") != "-")
)
emp_bronze = emp_bronze.withColumn("Name",
    when(col("Name").contains(","),  
         concat_ws(" ",
             split(col("Name"), ",").getItem(1),
             initcap(split(col("Name"), ",").getItem(0))
         ))
    .otherwise(col("Name"))
)
emp_bronze = emp_bronze.dropDuplicates(["Name","Department"])

#cleaning performance
emp_bronze = emp_bronze.withColumn("Performance", when((lower(col("Performance")) == "null")| (lower(col("Performance")) == "n/a"),"Unrecorded")\
    .otherwise(col("Performance")))\
    .filter(col("Performance").isNotNull())

#fixing the date to a common format
emp_bronze = emp_bronze.withColumn("date_mdy", to_date(col("Join_Date"), "M/d/yyyy")) \
       .withColumn("date_ymd", to_date(col("Join_Date"), "yyyy-MM-dd"))\
        .withColumn("new_date",when(col("date_mdy").isNotNull(), col("date_mdy"))\
        .otherwise(col("date_ymd")))\
        .withColumn("Join_Date_Clean", date_format("new_date", "yyyy-MM-dd"))\
        .drop("date_mdy", "date_ymd", "new_date", "Join_Date") \
       .withColumnRenamed("Join_Date_Clean", "Join_Date")

#Sorting and reset id
emp_bronze = emp_bronze.sort("ID")
emp_bronze = emp_bronze.withColumn("ID", row_number().over(Window.orderBy("ID")))

#writing to silver partitioned by department
emp_bronze.write.format(rtb_config["format"]).options(**rtb_config["options"]).mode(rtb_config["mode"]).partitionBy("Department").save(rtb_config["SilverEmpWriteAddress"])
dept_bronze.write.format(rtb_config["format"]).options(**rtb_config["options"]).mode(rtb_config["mode"]).save(rtb_config["SilverDepWriteAddress"])

#processing silver to gold
emp_silver = spark.read.options(**rtb_config["options2"]).format(rtb_config["format"]).load(rtb_config["SilverEmpWriteAddress"])
dept_silver = spark.read.options(**rtb_config["options2"]).format(rtb_config["format"]).load(rtb_config["SilverDepWriteAddress"])

emp_gold = emp_silver.join(dept_silver,emp_silver.Department==dept_silver.Department_Name,how="left").orderBy("ID")
emp_gold = emp_gold.drop("Department_Name")
emp_gold = emp_gold.select("Name","Department","Salary","Location","Join_Date").sort("Department")
emp_gold.show(50)

#writing to gold
emp_gold.write.format(rtb_config["format"]).options(**rtb_config["options2"]).mode(rtb_config["mode"]).save(rtb_config["GoldAddress"])
