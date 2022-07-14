
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
    .config("spark.hadoop.mapred.output.compress", "true") \
    .config("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec") \
    .config("spark.hadoop.mapred.output.compression.type", "gzip") \
    .getOrCreate()

# Загрузка данных
emp_df = spark.read.option("delimiter", "\t").option("header", "true").csv('/content/ForSparkDataLearn/employees/employees')
deps_df = spark.read.option("delimiter", ",").option("header", "true").csv('/content/ForSparkDataLearn/departments/departments')

# Просмотр структуры 
print('--- СТРУКТУРА ---')
print('Employees')
emp_df.printSchema()
emp_df.show(2, False)
print('Departments')
deps_df.printSchema()
deps_df.show(2, False)

# Решение задачи 
query = emp_df.join(deps_df, deps_df.DEPARTMENT_ID == emp_df.DEPARTMENT_ID, 'left') \
              .select(emp_df.LAST_NAME, emp_df.DEPARTMENT_ID, deps_df.DEPARTMENT_NAME)
query.write.format("avro").mode("overwrite").save("/content/data_out/task_3/")

# Проверка результата решения
print('--- ПРОВЕРКА РЕЗУЛЬТАТОВ ---')
spark.read.format("avro").option("compression", "gzip").load("/content/data_out/task_3").show(5, False)
