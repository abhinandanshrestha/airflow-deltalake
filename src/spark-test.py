from pyspark.sql import SparkSession

# Set your Spark master URL
spark_master = "spark://spark-master:7077"

# Create Spark session connected to your master
spark = SparkSession.builder \
    .master(spark_master) \
    .appName("SimpleApp") \
    .getOrCreate()

# Example data
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()

# Stop Spark session
spark.stop()
