package org.shikshalokam.job.user.service.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.user.service.domain.Event
import org.shikshalokam.job.user.service.functions.UserServiceFunction
import org.shikshalokam.job.util.FlinkUtil

import java.io.File

class UserServiceTask(config: UserServiceConfig, kafkaConnector: FlinkKafkaConnector) {
  println("Inside UserServiceTask class")

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    val source = kafkaConnector.kafkaJobRequestSource[Event](config.inputTopic)

    env.addSource(source).name(config.userServiceProducer)
      .uid(config.userServiceProducer).setParallelism(config.mlUserServiceParallelism).rebalance
      .process(new UserServiceFunction(config))
      .name(config.userServiceFunction).uid(config.userServiceFunction)
      .setParallelism(config.mlUserServiceParallelism)

    env.execute(config.jobName)
  }
}

object UserServiceTask {
  def main(args: Array[String]): Unit = {
    println("Starting up the User Service Job")
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("user-service.conf").withFallback(ConfigFactory.systemEnvironment()))
    val userServiceConfig = new UserServiceConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(userServiceConfig)
    val task = new UserServiceTask(userServiceConfig, kafkaUtil)
    task.process()
  }
}