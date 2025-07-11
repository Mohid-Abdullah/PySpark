from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,when,lower,try_to_timestamp,count,min,max
import os
os.environ["HADOOP_HOME"] = "D:\\hadoop-3.3.6"


spark = SparkSession.builder.appName("MedallionArchitecture").master("local[*]").getOrCreate()

#dummy data to practice on
raw_data = [
    {"user_id": "u1", "device": "iPhone", "login_time": "2025-07-08 12:00:00", "location": "New York"},
    {"user_id": "u2", "device": "Android", "login_time": "2025-07-08 12:05:00", "location": None},
    {"user_id": "u1", "device": "Laptop", "login_time": "2025-07-08 12:07:00", "location": "NY"},
    {"user_id": "u3", "device": "iPhone", "login_time": "not_a_time", "location": "Los Angeles"},
    {"user_id": None, "device": "Android", "login_time": "2025-07-08 12:10:00", "location": "LA"},
    {"user_id": "u4", "device": "Tablet", "login_time": "2025-07-08 12:15:00", "location": "New York"},
]

raw_df = spark.createDataFrame(raw_data)
raw_df.show()

raw_df.write.mode("overwrite").json("CleanDatasets/Cleaned/bronze/user_logins")

# Read from bronze
bronze_df = spark.read.json("CleanDatasets/Cleaned/bronze/user_logins")

silver_df = (
    bronze_df
    .filter(col("user_id").isNotNull())
    .filter(col("location").isNotNull())
    .withColumn("login_time", try_to_timestamp("login_time"))
    .filter(col("login_time").isNotNull())
    .withColumn("location", when(lower(col("location")).isin("ny", "new york"), "New York")
                           .when(lower(col("location")).isin("la", "los angeles"), "Los Angeles")
                           .otherwise(col("location")))
)


silver_df.write.mode("overwrite").parquet("CleanDatasets/Cleaned/silver/user_logins_cleaned")


silver_df = spark.read.parquet("CleanDatasets/Cleaned/silver/user_logins_cleaned")

gold_df = silver_df.groupBy("user_id", "location").agg(
    count("*").alias("total_logins"),
    min("login_time").alias("first_login"),
    max("login_time").alias("last_login")
)

gold_df.write.mode("overwrite").parquet("CleanDatasets/Cleaned/gold/user_login_summary")

gold_df.show()