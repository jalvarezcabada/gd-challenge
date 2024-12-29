from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)

SPARK = (
    SparkSession.builder
    .appName("gd-challenge")
    .config("spark.executor.instances", 6)
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "3g")
    .getOrCreate()
)

EVENTS_PATH = "/opt/spark/data/bronze/events.csv.gz"
FREE_SMS_PATH = "/opt/spark/data/bronze/free_sms_destinations.csv.gz"

EVENTS_SMS_PATH = "/opt/spark/data/silver/events_sms"
TOP_USERS_PATH = "/opt/spark/data/gold/top_users"

HISTOGRAM_PATH = "/opt/spark/images/calls_per_hour_histogram.png"