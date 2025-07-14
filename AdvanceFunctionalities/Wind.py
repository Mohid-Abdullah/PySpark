#practice code for delta lake, udf, window functions, time travel, etc.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr,avg,when,lower,sum,month,year,udf
from pyspark.sql.types import StringType, StructField,IntegerType,DateType,StructType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

session = SparkSession.builder.appName("Wind").master('local[*]').config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

schema = StructType([
    StructField("Name",StringType(),True),
    StructField("Salary",IntegerType(),True),
    StructField("Departments",StringType(),True),
    StructField("Date",DateType(),True)
])
#creating a sipmle udf
# def label_income(salary):
#     if salary > 30000:
#         return "Rich"
#     elif salary > 18000:
#         return "Upper_Middle_Class"
#     else:
#         return "Lower_Middle_Class"

# df = session.read.option("header", True).schema(schema).csv("Demo.csv") #manually infering schema

df = session.read.option("header", True).option("inferSchema", True).csv("Demo.csv")
df = df.withColumn("Departments",when(lower(col('Departments')) == "big data",'Big_Data')\
    .when(lower(col('Departments')) == "data science",'Data_Science').otherwise(col('Departments')))

df.show()
prt = Window.partitionBy("Name")

df = df.withColumn("Total_Salary",sum(col("Salary")).over(prt)).sort("Total_Salary",ascending=False)
df = df.withColumn("Month",month(col("Date"))).withColumn("Year",year(col("Date")))
df.show()

df.write.mode("overwrite").partitionBy("Year","Month","Departments").parquet("partitions")
bd_df = session.read.parquet("partitions/Year=2024/Month=3/").sort("Total_Salary",ascending=False).filter(col("Departments") != lower(lit("data_science")))
# bd_df.show()

# delta table
df.write.option("mergeSchema",True).format("delta").mode("overwrite").save("delta_table/")
delta_df = DeltaTable.forPath(session,"delta_table/")
delta_df.update(condition="Name = 'Ali' AND Departments = 'Big_Data'",set={"Salary": lit(12000)})
delta_df.delete("Name = 'Ali' AND Departments = 'Data_Science'" )

# Read the updated Delta table
new_df = session.read.format("delta").load("delta_table/")
new_df.show()

#time travel (versioning)
delta_df.history().show()
df_v1 = session.read.format("delta").option("versionAsOf", 1).load("delta_table/")

# Add New_Salary column to the existing Delta table
session.sql("ALTER TABLE delta.`delta_table/` ADD COLUMNS (New_Salary INT)")

delta_df.update(
    condition="1=1",  # Update all rows
    set={"New_Salary": col("Salary") + 1000}
)

# Read the modified table
modified_df = session.read.format("delta").load("delta_table/")
modified_df.show()
df_v1 = session.read.format("delta").option("versionAsOf", 1).load("delta_table/")
df_v1.show()
