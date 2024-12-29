from pyspark.sql.functions import col, when, lit

from config import SPARK, LOGGER, EVENTS_SMS_PATH, EVENTS_PATH, FREE_SMS_PATH
from utils import read_csv_file, write_parquet_file

# Read the CSV file
events_df = read_csv_file(spark=SPARK, path=EVENTS_PATH)
free_sms_df = read_csv_file(spark=SPARK, path=FREE_SMS_PATH)

# Filter out records where id_source or id_destination are null
events_df = events_df.filter(
    (col("id_source").isNotNull()) & (col("id_destination").isNotNull())
)

# Perform a join with free SMS destinations
events_sms = events_df.join(
    free_sms_df, events_df.id_destination == free_sms_df.id, "left_outer"
)

# Calculate SMS costs
events_sms = events_sms.withColumn(
    "sms_cost",
    when(col("id").isNotNull(), lit(0.0))  # $0.0 for free destinations
    .when(col("region").between(1, 5), col("sms") * lit(1.5))  # $1.5 for regions 1-5
    .when(col("region").between(6, 9), col("sms") * lit(2.0))  # $2.0 for regions 6-9
    .otherwise(lit(0.0)),  # Default cost is $0
)

write_parquet_file(
    dataframe=events_sms, path=EVENTS_SMS_PATH, mode="overwrite", compression="gzip"
)

LOGGER.info(f"Dataset successfully saved in path: {EVENTS_SMS_PATH}")

# Calculate the total SMS cost
total_facturado = events_sms.agg({"sms_cost": "sum"}).collect()[0][0]

LOGGER.info(f"The total SMS revenue is: ${total_facturado:.2f}")
