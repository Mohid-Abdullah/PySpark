from pyspark.sql import SparkSession

session = SparkSession.builder.appName("Read JSONs").master("local[*]").getOrCreate()

#single line json
singledf = session.read.json(r"Reading_JSONS\record_oriented.json")
singledf.show()

#Multi line json
multidf = session.read.option("multiLine", True).json(r"Reading_JSONS\array_of_records.json")
multidf.show()

#Nested Multi line json
nesteddf = session.read.option("multiLine", True).json(r"Reading_JSONS\nested_records.json")
nesteddf = nesteddf.select("name", "age", "location.city","location.zip", "skills")
nesteddf.show()
