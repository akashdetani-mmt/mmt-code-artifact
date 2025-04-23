from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("ClickstreamOrderEnrichment").getOrCreate()

clickstream_df_col = spark.read.json("Dataset/clickstream.json", multiLine=True).columns

order_df_col = spark.read.json("Dataset/order.json", multiLine=True).columns

combined_col = set(clickstream_df_col+order_df_col)

clickstream_df = spark.read.json("Dataset/clickstream.json", multiLine=True)
order_df = spark.read.json("Dataset/order.json", multiLine=True)

clickstream_df = clickstream_df.withColumn("booking_details_id", f.col("booking_details.id"))

order_df = order_df.withColumn("booking_details_id", f.col("booking_details.id")).withColumn("amount", f.col("booking_details.amount")).withColumn("location", f.col("booking_details.location")).withColumn("device_id", f.col("booking_details.device_id")).select("booking_details_id", "amount", "location", "device_id")

join_df = clickstream_df.join(order_df, 'booking_details_id', 'left')

# df = join_df.select(combined_col)
# df.show(truncate=False)
