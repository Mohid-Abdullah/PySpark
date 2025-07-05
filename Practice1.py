import warnings
warnings.filterwarnings("ignore", category=UserWarning)
from pyspark.sql import SparkSession
import pandas as pd

# ne = {'Name': ['Mohid','abdullah','ghumman'],
#                   'Age': [25, 32,21],
#                   'experience': [2,4,5]}
# pddf = pd.DataFrame(ne)
# pddf.to_csv('test2.csv',index=False)

spark = SparkSession.builder.appName('2').master("local[*]").config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j2.properties").getOrCreate()
psdf =spark.read.option('header','true').csv('test2.csv',inferSchema=True)
psdf.show()
psdf.printSchema()
print(psdf.columns)
psdf.select('Name').show()
psdf.select(['Age','experience']).show()
print(psdf.describe().show())
psdf = psdf.withColumn('experienceToAgeRatio',(psdf['experience']/psdf['Age'])*100)
psdf.show()
psdf = psdf.drop('experience')
psdf = psdf.drop('Age')
psdf = psdf.withColumnRenamed('experienceToAgeRatio','EARatio')
psdf.show()