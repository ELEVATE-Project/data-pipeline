package org.shikshalokam.job.project.creator.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.project.creator.domain.Event
import org.shikshalokam.job.project.creator.functions.ProjectDashboardFunction
import org.shikshalokam.job.util.FlinkUtil

import java.io.File

class ProjectDashboardTask(config: ProjectDashboardConfig, kafkaConnector: FlinkKafkaConnector) {
  println("inside ProjectDashboardTask class")

  private val serialVersionUID = -7729362727131516112L
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    val source = kafkaConnector.kafkaJobRequestSource[Event](config.inputTopic)

    env.addSource(source).name(config.mlProjectsConsumer)
      .uid(config.mlProjectsConsumer).setParallelism(config.mlProjectsParallelism).rebalance
      .process(new ProjectDashboardFunction(config))
      .name(config.projectsDashboardFunction).uid(config.projectsDashboardFunction)
      .setParallelism(config.mlProjectsParallelism)

    env.execute(config.jobName)
  }
}

object ProjectDashboardTask {
  println("inside ProjectDashboardTask object")
  def main(args: Array[String]): Unit = {
    println("Starting up the Project Dashboard creation Job")
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("project-dashboard.conf").withFallback(ConfigFactory.systemEnvironment()))
    val projectDashboardConfig = new ProjectDashboardConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(projectDashboardConfig)
    val task = new ProjectDashboardTask(projectDashboardConfig, kafkaUtil)
    task.process()
  }
}