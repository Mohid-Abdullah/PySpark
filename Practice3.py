from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
import pandas as pd

data = {
    'Name': ['Mohammed', 'Mohammed', 'Ali', 'Mohammed', 'Ali', 'Omar', 'Omar', 'Omar', 'Yusuf', 'Yusuf'],
    'Departments': ['Data Science', 'IOT', 'Big Data', 'Big Data', 'Data Science', 'Data Science', 'IOT', 'Big Data', 'Data Science', 'Big Data'],
    'salary': [10000, 5000, 4000, 4000, 3000, 20000, 10000, 5000, 10000, 2000]
}

df = pd.DataFrame(data)
df.to_csv('Practice3.csv',index=False)


session = SparkSession.builder.appName('Practice3').getOrCreate()
file = session.read.csv('Practice3.csv',header=True,inferSchema=True)
file.show()

file.filter('salary<=2500').show()
file.filter('salary<=2500').select(['Name','Departments']).show()
file.filter(file['salary']<=2500).select(['Name','Departments']).show()
file.filter((file['salary']<=2500) & (file['Departments']=='Data Science')).select(['Name','Departments']).show()
file.filter((file['salary']<=2500) & ~(file['Departments']=='Data Science')).select(['Name','Departments']).show()

file.groupBy('Name').sum('salary').show()
file.groupBy('Departments').avg ('salary').show()
file.groupBy('Departments').count().show()








