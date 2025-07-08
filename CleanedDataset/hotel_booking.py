from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,when,to_date,date_format
from pyspark.sql.types import IntegerType

#link to dataset
#"#https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand/download?datasetVersionNumber=1"

session = SparkSession.builder.appName('Hotel').master('local[*]').getOrCreate()
df = session.read.csv(r"CleanDatasets\hotel_bookings.csv",header=True,inferSchema=True)

before_outliers = df.count()
print(before_outliers)
df.printSchema()

# df.select("children").distinct().show(10)
# df.select("reservation_status_date").distinct().show(10)

#checking for null values
nullCols = df.select([sum(when(col(c).isNull(),1).otherwise(0)).alias(c)for c in df.columns])
nullDict = nullCols.collect()[0].asDict()
for a,b in nullDict.items():
    if b == 0:
        continue
    else:
        print(f"{a}:{b}")

#removing outliers
df = df.filter((col("adr") < 1000)&(col("adr") > 0))

#i noticed that the children column has string "NA" values in an int coloumn,fixing that
df = df.withColumn(
    "children",
    when(col("children") == "NA", None).otherwise(col("children"))
) 

df = df.withColumn("children", col("children").cast(IntegerType()))

df = df.filter((df.adults > 0) & (df.children >= 0) & (df.babies >= 0))

#changing the reservation_status_date format just for fun from yyyy-mm-dd to dd-mm-yyyy
df = df.withColumn("reservation_status_date", date_format(col("reservation_status_date"), "dd/MM/yyyy"))
df.select("reservation_status_date").distinct().show(10)


print(100-((df.count()/before_outliers )*100))
df.limit(100000).toPandas().to_csv(r"CleanDatasets\Cleaned\hotel_bookings_cleaned.csv", index=False)
