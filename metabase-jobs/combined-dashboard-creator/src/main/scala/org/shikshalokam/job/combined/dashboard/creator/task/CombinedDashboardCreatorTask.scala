package org.shikshalokam.job.combined.dashboard.creator.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.shikshalokam.job.combined.dashboard.creator.domain._
import org.shikshalokam.job.combined.dashboard.creator.functions.mentoring.MentoringMetabaseDashboardFunction
import org.shikshalokam.job.combined.dashboard.creator.functions.observation.ObservationMetabaseDashboardFunction
import org.shikshalokam.job.combined.dashboard.creator.functions.project.ProjectMetabaseDashboardFunction
import org.shikshalokam.job.combined.dashboard.creator.functions.survey.SurveyMetabaseDashboardFunction
import org.shikshalokam.job.combined.dashboard.creator.functions.user.UserMetabaseDashboardFunction
import org.shikshalokam.job.combined.dashboard.creator.domain.UserMappingEvent
import org.shikshalokam.job.combined.dashboard.creator.functions.userMapping.{ProgramServiceFunction, UserServiceFunction}
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.util.FlinkUtil
import java.io.File

object CombinedDashboardCreatorTask {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val config: Config = if (params.has("config.file.path")) {
      ConfigFactory.parseFile(new File(params.get("config.file.path"))).resolve()
    } else {
      ConfigFactory.load("combined-dashboard-creator.conf").resolve()
    }

    val dashboardConfig: CombinedDashboardCreatorConfig = new CombinedDashboardCreatorConfig(config)
    val kafkaConnector: FlinkKafkaConnector = new FlinkKafkaConnector(dashboardConfig)

    runJob(dashboardConfig, kafkaConnector)
  }

  def runJob(config: CombinedDashboardCreatorConfig, kafkaConnector: FlinkKafkaConnector): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    // Mentoring
    if (config.config.hasPath("kafka.input.mentoring.enabled") && config.config.getBoolean("kafka.input.mentoring.enabled")) {
      val mentoringSource = kafkaConnector.kafkaJobRequestSource[MentoringEvent](config.mentoringInputTopic)
      env.addSource(mentoringSource)
        .name("mentoring-dashboard-consumer").uid("mentoring-dashboard-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new MentoringMetabaseDashboardFunction(config))
        .name("mentoring-dashboard-processor").uid("mentoring-dashboard-processor")
        .setParallelism(config.parallelism)
    }

    // Observation
    if (config.config.hasPath("kafka.input.observation.enabled") && config.config.getBoolean("kafka.input.observation.enabled")) {
      val observationSource = kafkaConnector.kafkaJobRequestSource[ObservationEvent](config.observationInputTopic)
      env.addSource(observationSource)
        .name("observation-dashboard-consumer").uid("observation-dashboard-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new ObservationMetabaseDashboardFunction(config))
        .name("observation-dashboard-processor").uid("observation-dashboard-processor")
        .setParallelism(config.parallelism)
    }

    // Project
    if (config.config.hasPath("kafka.input.project.enabled") && config.config.getBoolean("kafka.input.project.enabled")) {
      val projectSource = kafkaConnector.kafkaJobRequestSource[ProjectEvent](config.projectInputTopic)
      env.addSource(projectSource)
        .name("project-dashboard-consumer").uid("project-dashboard-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new ProjectMetabaseDashboardFunction(config))
        .name("project-dashboard-processor").uid("project-dashboard-processor")
        .setParallelism(config.parallelism)
    }

    // Survey
    if (config.config.hasPath("kafka.input.survey.enabled") && config.config.getBoolean("kafka.input.survey.enabled")) {
      val surveySource = kafkaConnector.kafkaJobRequestSource[SurveyEvent](config.surveyInputTopic)
      env.addSource(surveySource)
        .name("survey-dashboard-consumer").uid("survey-dashboard-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new SurveyMetabaseDashboardFunction(config))
        .name("survey-dashboard-processor").uid("survey-dashboard-processor")
        .setParallelism(config.parallelism)
    }

    // User
    if (config.config.hasPath("kafka.input.user.enabled") && config.config.getBoolean("kafka.input.user.enabled")) {
      val userSource = kafkaConnector.kafkaJobRequestSource[UserEvent](config.userInputTopic)
      env.addSource(userSource)
        .name("user-dashboard-consumer").uid("user-dashboard-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new UserMetabaseDashboardFunction(config))
        .name("user-dashboard-processor").uid("user-dashboard-processor")
        .setParallelism(config.parallelism)
    }

    // User Service
    if (config.config.hasPath("kafka.input.userservice.enabled") && config.config.getBoolean("kafka.input.userservice.enabled")) {
      // Create a UserServiceConfig wrapper for compatibility
      val userServiceSource = kafkaConnector.kafkaJobRequestSource[UserMappingEvent](config.userServiceInputTopic)
      val userServiceStream = env.addSource(userServiceSource)
        .name("user-service-consumer").uid("user-service-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new UserServiceFunction(config))
        .name("user-service-processor").uid("user-service-processor")
        .setParallelism(config.parallelism)
      
      // Sink notification events to Kafka
      val notificationSideOutput = userServiceStream.getSideOutput(config.userServiceOutputTag)
      notificationSideOutput
        .addSink(kafkaConnector.kafkaStringSink(config.notificationOutputTopic))
        .name("notification-service-producer").uid("notification-service-producer")
        .setParallelism(config.parallelism)
    }

    // Program Service
    if (config.config.hasPath("kafka.input.programservice.enabled") && config.config.getBoolean("kafka.input.programservice.enabled")) {
      val programServiceSource = kafkaConnector.kafkaJobRequestSource[UserMappingEvent](config.programServiceInputTopic)
      env.addSource(programServiceSource)
        .name("program-service-consumer").uid("program-service-consumer")
        .setParallelism(config.parallelism).rebalance
        .process(new ProgramServiceFunction(config))
        .name("program-service-processor").uid("program-service-processor")
        .setParallelism(config.parallelism)
    }

    env.execute("Combined Dashboard Creator Job")
  }
}
