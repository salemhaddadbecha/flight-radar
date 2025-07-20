from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flight_Radar_Pipeline").master("local[4]").config("spark.driver.memory", "4g").getOrCreate()