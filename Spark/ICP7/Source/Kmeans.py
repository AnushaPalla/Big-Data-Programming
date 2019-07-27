from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import sys
import os
os.environ["SPARK_HOME"] = "C:\spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
# Creating spark session
spark = SparkSession.builder.appName("ICP7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define input path
input_path = "C:\\Users\\Anusha Reddy\\PycharmProjects\\Lab"

# Load data and select feature and label columns
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load(input_path + "\\diabetic_data.csv")
data = data.select("admission_type_id", "discharge_disposition_id", "admission_source_id", "time_in_hospital", "num_lab_procedures")

# Create vector assembler for feature columns
assembler = VectorAssembler(inputCols=data.columns, outputCol="features")
data = assembler.transform(data)
# k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(data)
# Make predictions
predictions = model.transform(data)
# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)