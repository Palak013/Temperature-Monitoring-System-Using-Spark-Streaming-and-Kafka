
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

object TemperatureMonitoring {
  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("TemperatureMonitoring")
      .master("local[*]")
      .getOrCreate()

    // Set up Kafka consumer parameters
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "temperature-monitoring-group",
      "auto.offset.reset" -> "earliest"
    )

    // Create a streaming context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    // Subscribe to the Kafka topic
    val topics = Array("temperature-sensors")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Process Kafka stream and filter for high temperatures
    kafkaStream.foreachRDD { rdd =>
      val data = rdd.map(record => {
        val value = record.value()
        val json = ujson.read(value)
        val sensorId = json("sensor_id").str
        val temp = json("temp").num
        (sensorId, temp)
      })

      val highTempData = data.filter(_._2 > 80)

      highTempData.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach { case (sensorId, temp) =>
          println(s"High temperature alert! Sensor: $sensorId, Temperature: $temp")
        }
      }
    }

    // Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }
}
