from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import uuid

spark = SparkSession.builder.appName("Sessionization").getOrCreate()

clickstream_df = spark.read.json("Dataset/clickstream.json", multiLine=True)

clickstream_df = clickstream_df.withColumn("event_timestamp", f.col("event_timestamp").cast("long"))

w = Window.partitionBy("uuid").orderBy("event_timestamp")

clickstream_df = clickstream_df.withColumn("time_lag", f.lag("event_timestamp").over(w))
time_diff_df = clickstream_df.withColumn("time_diff", f.col("event_timestamp") - f.col("time_lag")).drop("time_lag")

new_session_df = time_diff_df.withColumn(
    "is_new_session",
    f.when(f.col("time_diff") > 1800000, 1).otherwise(0)
)

session_id_df = new_session_df.withColumn("session_id", f.when(f.col("is_new_session") == 1, f.lit(uuid.uuid4().hex)).otherwise(f.lit(None)))

session_window = Window.partitionBy("uuid")
clickstream_session_df = session_id_df.withColumn("session_start_time_ms",f.min("event_timestamp").over(session_window)).withColumn("session_end_time_ms",f.max("event_timestamp").over(session_window))
# clickstream_session_df.show(truncate=False)
