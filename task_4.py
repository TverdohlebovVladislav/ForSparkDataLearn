
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import  SparkContext
from pyspark.sql import functions as f

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.2.1-bin-hadoop3.2"

appName = "PySpark Task 1"
master = 'local[*]'

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .enableHiveSupport() \
    .appName(appName) \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.0") \
    .getOrCreate()

# Загрузка данных
emp_df = spark.read.option("delimiter", "\t").option("header", "true").csv('/content/ForSparkDataLearn/employees/employees')
managers_df = spark.read.option("delimiter", "\t").option("header", "true").csv('/content/ForSparkDataLearn/employees/employees')

# Просмотр структуры 
print('--- СТРУКТУРА ---')
print('Employees')
emp_df.printSchema()
emp_df.show(2, False)

# Решение задачи 
query = emp_df.join(managers_df, managers_df.EMPLOYEE_ID == emp_df.MANAGER_ID) \
              .select(
                  emp_df.LAST_NAME.alias('Employee'), 
                  emp_df.EMPLOYEE_ID.alias('EmpID'), 
                  managers_df.LAST_NAME.alias('Manager'),
                  emp_df.MANAGER_ID.alias('MgrID')      
                )
query.write.format("avro").option("compression", "snappy").mode("overwrite").save("/content/data_out/task_4")

# Проверка результата решения
print('--- ПРОВЕРКА РЕЗУЛЬТАТОВ ---')
spark.read.format("avro").option("compression", "snappy").load("/content/data_out/task_4").show(5, False)
