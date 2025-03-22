from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format
from pyspark.sql.types import StructType, StringType, IntegerType

# === Config ===
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "movie_ratings"
NETFLIX_FILE_PATH = "data/netflix.csv"
WRITE_INTERVAL = "30 seconds"

# === Spark Session ===
spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Kafka message schema ===
kafka_schema = StructType() \
    .add("name", StringType()) \
    .add("movie", StringType()) \
    .add("timestamp", StringType()) \
    .add("rating", IntegerType())

# === Read from Kafka ===
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kafka_schema).alias("data")) \
    .selectExpr(
        "data.name as name",
        "data.movie as movie",
        "data.timestamp as raw_timestamp",
        "data.rating as user_rating"
    ) \
    .withColumn("timestamp", to_timestamp("raw_timestamp", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("hour", date_format("timestamp", "yyyy-MM-dd HH:00"))


# === Load Netflix CSV ===
netflix_df = spark.read.option("header", True).csv(NETFLIX_FILE_PATH)

# === Join by movie title ===
enriched_df = parsed_df.join(netflix_df, parsed_df.movie == netflix_df.title, "left")

# === Prepare final dataframe for Cassandra ===
final_df = enriched_df.select(
    col("name").alias("username"),
    col("hour"),
    col("timestamp"),
    col("movie"),
    col("user_rating").alias("rating"),
    col("title"),
    col("director"),
    col("release_year").cast("int"),
    col("duration").cast("int")
)

# === Write to Cassandra ===
def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="ratings_by_user_hour", keyspace="netflix") \
        .save()

# === Start streaming with foreachBatch ===
query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .trigger(processingTime=WRITE_INTERVAL) \
    .start()

query.awaitTermination()
