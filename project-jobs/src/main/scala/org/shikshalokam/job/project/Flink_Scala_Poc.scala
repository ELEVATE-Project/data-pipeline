package org.shikshalokam.job.project

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

object Flink_Scala_Poc {
  def main(args: Array[String]): Unit = {
    println("hi")
    // Set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    println("bye")
    // Configure Kafka consumer
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-consumer-group")

    println(properties)

    val kafkaConsumer = new FlinkKafkaConsumer[String]("dev.test.kafka", new SimpleStringSchema(), properties)

    // Add the consumer as a source to the Flink job
    val stream = env.addSource(kafkaConsumer)

    // Initialize Jackson ObjectMapper
    val mapper = new ObjectMapper()

    // Process the stream
    stream.map { value =>
      // Parse JSON and extract the "Name" key
      try {
        val json: JsonNode = mapper.readTree(value)
        val message = if (json.has("Name")) json.get("Name").asText() else "Key 'Name' not found"
        s"Processed: $message"
      } catch {
        case e: Exception => s"Invalid JSON: $value"
      }
    }.print()

    // Execute the Flink job
    env.execute("Flink Scala Kafka Streaming Job POC")
  }
}
