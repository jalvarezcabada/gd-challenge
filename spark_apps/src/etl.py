from pyspark.sql import SparkSession

# Crear una sesión Spark conectada al maestro del clúster
spark = SparkSession.builder \
    .appName("gd-challenge") \
    .getOrCreate()

events_path = "/opt/spark/data/raw/events.csv.gz"
free_sms_path = "/opt/spark/data/raw/free_sms_destinations.csv.gz"

events_df = spark.read.option("header", "true").csv(events_path)
free_sms_df = spark.read.option("header", "true").csv(free_sms_path)

events_df.show(5)
free_sms_df.show(5)
