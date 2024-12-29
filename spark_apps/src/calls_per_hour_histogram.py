from pyspark.sql.functions import col, sum
import matplotlib.pyplot as plt

from config import SPARK, EVENTS_PATH, HISTOGRAM_PATH
from utils import read_csv_file

# Read the CSV file
events_df = read_csv_file(spark=SPARK, path=EVENTS_PATH)

# Filter records with null id_source or id_destination
df_filtered = events_df.filter(
    col("id_source").isNotNull() & col("id_destination").isNotNull()
)

# Convert columns to appropriate types (e.g., hour and calls should be integers)
df_filtered = df_filtered.withColumn("hour", col("hour").cast("int")).withColumn(
    "calls", col("calls").cast("int")
)

# Group by hour and sum the number of calls
df_hourly_calls = df_filtered.groupBy("hour").agg(sum("calls").alias("total_calls"))

# Convert to Pandas for plotting
pandas_df = df_hourly_calls.toPandas()

# Plot the histogram of calls per hour
plt.figure(figsize=(10, 6))
plt.bar(pandas_df["hour"], pandas_df["total_calls"], color="skyblue")
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Calls")
plt.title("Number of Calls Made Per Hour of the Day")
plt.xticks(range(0, 24))
plt.grid(True)
plt.savefig(HISTOGRAM_PATH)
