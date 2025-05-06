import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object TemperatureMonitor {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("IoT Temperature Monitor with Kafka")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Kafka configuration
    val kafkaBootstrapServers = "localhost:9092"
    val inputTopic = "temperature-sensors"
    val outputTopic = "alert-temperature"

    // Define schema for incoming JSON
    val schema = new StructType()
      .add("sensor_id", StringType)
      .add("room_id", StringType)
      .add("timestamp", StringType)
      .add("temp", DoubleType)
      .add("location", StringType)

    // Read stream from Kafka
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .load()

    // Extract JSON string
    val messageDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) AS message")

    // Parse JSON and apply schema
    val tempDF = messageDF
      .select(from_json(col("message"), schema).as("data"))
      .select("data.*")

    // Filter high temperatures and categorize
    val highTempDF = tempDF
      .filter($"temp" > 80.0)
      .withColumn("alert_level",
        when($"temp" > 90, "CRITICAL")
        .when($"temp" > 85, "SEVERE")
        .otherwise("WARNING"))
      .withColumn("processing_time", current_timestamp())

    // Prepare output for Kafka
    val kafkaOutputDF = highTempDF
      .selectExpr("CAST(sensor_id AS STRING) AS key", "to_json(struct(*)) AS value")

    // Write to Kafka output topic
    val query = kafkaOutputDF.writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .option("checkpointLocation", "file:///tmp/spark-kafka-checkpoint") // use a valid path
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
