from pyspark.sql.functions import col, md5, concat_ws

from config import SPARK, LOGGER, EVENTS_SMS_PATH, TOP_USERS_PATH
from utils import read_parquet_file, write_parquet_file

# Load the Parquet dataset
events_sms = read_parquet_file(spark=SPARK, path=EVENTS_SMS_PATH)

# Sum the total SMS cost per source user
user_billing = (
    events_sms.groupBy("id_source")
    .agg({"sms_cost": "sum"})
    .withColumnRenamed("sum(sms_cost)", "total_billed")
)

# Sort by billing amount and select the top 100 users
top_users = user_billing.orderBy(col("total_billed").desc()).limit(100)

# Create a column with hashed IDs using MD5
top_users = top_users.withColumn("id_hashed", md5(concat_ws("", col("id_source"))))

# Save the dataset in Parquet format with gzip compression
write_parquet_file(
    dataframe=top_users, path=TOP_USERS_PATH, mode="overwrite", compression="gzip"
)

LOGGER.info(f"Dataset successfully saved in path: {TOP_USERS_PATH}")
