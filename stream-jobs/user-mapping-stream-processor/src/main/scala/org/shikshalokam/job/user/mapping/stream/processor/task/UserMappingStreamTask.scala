package org.shikshalokam.job.user.mapping.stream.processor.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.user.mapping.stream.processor.domain.ObservationEvent
import org.shikshalokam.job.user.mapping.stream.processor.functions.UserMappingStreamFunction
import org.shikshalokam.job.util.FlinkUtil

import java.io.File

class UserMappingStreamTask(config: UserMappingStreamConfig, kafkaConnector: FlinkKafkaConnector){

  private val serialVersionUID = -7729362727131516112L
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[ObservationEvent] = TypeExtractor.getForClass(classOf[ObservationEvent])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaJobRequestSource[ObservationEvent](config.inputTopic)

    val progressStream = env.addSource(source).name(config.usersStreamConsumer)
      .uid(config.usersStreamConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new UserMappingStreamFunction(config))
      .name(config.usersStreamFunction).uid(config.usersStreamFunction)
      .setParallelism(config.usersStreamParallelism)

    progressStream.getSideOutput(config.eventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.outputTopic))
      .name(config.metabaseDashboardProducer)
      .uid(config.metabaseDashboardProducer)
      .setParallelism(config.metabaseDashboardParallelism)

    // sink for mentoring dashboard events (new)
    progressStream.getSideOutput(config.mentoringEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.mentoringOutputTopic))
      .name(config.mentoringDashboardProducer)
      .uid(config.mentoringDashboardProducer)
      .setParallelism(config.metabaseDashboardParallelism)

    env.execute(config.jobName)
  }
}

object UserMappingStreamTask {
  def main(args: Array[String]): Unit = {
    println("Starting up the User Mapping Stream Job")
    val parameterTool = ParameterTool.fromArgs(args)
    val configFilePath = Option(parameterTool.get("config.file.path"))
    
    // Load config with proper precedence: system environment > config file > base config
    val baseConfig = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path))
    }.getOrElse(ConfigFactory.load("user-mapping-stream.conf"))
    
    // System environment variables take precedence, then config file, then base config
    val config = ConfigFactory.systemEnvironment()
      .withFallback(baseConfig)
      .resolve()
    
    val userMappingStreamConfig = new UserMappingStreamConfig(config)
    
    // Log Kafka configuration for debugging
    val brokerServers = userMappingStreamConfig.kafkaBrokerServers
    println(s"[UserMappingStreamTask] ========================================")
    println(s"[UserMappingStreamTask] Kafka Configuration:")
    println(s"[UserMappingStreamTask]   Broker Servers: $brokerServers")
    println(s"[UserMappingStreamTask]   Group ID: ${userMappingStreamConfig.groupId}")
    println(s"[UserMappingStreamTask]   Input Topic: ${userMappingStreamConfig.inputTopic}")
    println(s"[UserMappingStreamTask]   Output Topic: ${userMappingStreamConfig.outputTopic}")
    println(s"[UserMappingStreamTask] ========================================")
    
    // Validate Kafka broker servers configuration
    if (brokerServers == null || brokerServers.trim.isEmpty) {
      throw new IllegalArgumentException(
        "Kafka broker-servers is not configured. Please set kafka.broker-servers in your config file " +
        "or set KAFKA_BROKER_SERVERS environment variable."
      )
    }
    
    // Check if broker-servers is the default Docker value and warn if it might not resolve
    if (brokerServers == "kafka:9092") {
      println(s"[UserMappingStreamTask] WARNING: Using default Docker broker-servers 'kafka:9092'. " +
        "If running outside Docker, this may not resolve. Set kafka.broker-servers in config or KAFKA_BROKER_SERVERS env var.")
    }
    
    // Provide troubleshooting information
    println(s"[UserMappingStreamTask] Troubleshooting:")
    println(s"[UserMappingStreamTask]   If you see 'TimeoutException: Timeout expired while fetching topic metadata':")
    println(s"[UserMappingStreamTask]   1. Verify Kafka is running: Check if Kafka broker is accessible at $brokerServers")
    println(s"[UserMappingStreamTask]   2. Test connection: Try 'telnet <host> <port>' or 'nc -zv <host> <port>'")
    println(s"[UserMappingStreamTask]   3. Check firewall: Ensure port is not blocked")
    println(s"[UserMappingStreamTask]   4. Verify address: Confirm the broker address is correct in your config")
    println(s"[UserMappingStreamTask]   5. Check topics exist: Verify topics '${userMappingStreamConfig.inputTopic}' and '${userMappingStreamConfig.outputTopic}' exist")
    println(s"[UserMappingStreamTask]")
    
    val kafkaUtil = new FlinkKafkaConnector(userMappingStreamConfig)
    val task = new UserMappingStreamTask(userMappingStreamConfig, kafkaUtil)
    task.process()
  }
}