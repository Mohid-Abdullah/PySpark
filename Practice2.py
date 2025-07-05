from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
import pandas as pd

# new = {'Name':['Mohid','Abdullah','Ahmad','Haris',None],
#        'Age':[21,20,19, None,13],
#        'Experience':[2,3,1,None,2],
#        'Salary':[2000,3500,2000,2500,4000]}
# pdf = pd.DataFrame(new)
# pdf.to_csv('Practice2.csv',index=False)

session = SparkSession.builder.appName('Practice2').master("local[*]").getOrCreate()
file = session.read.csv("Practice2.csv",header=True,inferSchema=True)
file.show()
file.na.drop().show() #drops rows with atleast one null value
file.na.drop(how='all').show() #drop rows with all null values
file.na.drop(how='all',thresh=3).show() #drop all rows where there are less than 3 non-null values
file.na.drop(how='any',subset=['Name'],thresh=3).show()#drop where subset coloumn is null

file.na.fill('filled values',subset=['Name']).show()

#filling coloumns with mean
mean_Age = file.select(mean(col("Age"))).first()[0]
mean_Ex = file.select(mean(col("Experience"))).first()[0]
file.fillna(mean_Age, subset=["Age"]).show()
file.fillna(mean_Ex, subset=["Experience"]).show()