from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

spark = SparkSession.builder.appName("NYC").master("local[*]").getOrCreate()

#link to the dataset
#"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

df = spark.read.parquet(r"CleanDatasets\nyc.parquet", header=True, inferSchema=True)
df.show(5)
df.printSchema()
before = df.count()
print(before)

df = df.dropna(subset=["passenger_count"])
df = df.filter((col("passenger_count") > 0) & ((col("passenger_count") < 7)))
df = df.filter((col("fare_amount") > 0) & (col("trip_distance") > 0))

after = df.count()
print(before-after)

#checking all coloumns with null value
nullCols = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
null_Cols_dict = nullCols.collect()[0].asDict()

for col_name, null_count in null_Cols_dict.items():
    if (null_count == 0):
        continue
    else:
        print(f"{col_name}: {null_count}")
