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
import org.shikshalokam.job.util.{FlinkUtil, MetabaseUtil, PostgresUtil}

import java.io.File

object CombinedDashboardCreatorTask {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val baseConfig: Config = if (params.has("config.file.path")) {
      ConfigFactory.parseFile(new File(params.get("config.file.path"))).resolve()
    } else {
      ConfigFactory.load("unified-common.conf").resolve()
    }

    val dashboardConfig: CombinedDashboardCreatorConfig = new CombinedDashboardCreatorConfig(baseConfig)
    val kafkaConnector: FlinkKafkaConnector = new FlinkKafkaConnector(dashboardConfig)

    runJob(dashboardConfig, kafkaConnector)
  }

  def runJob(config: CombinedDashboardCreatorConfig, kafkaConnector: FlinkKafkaConnector, postgresUtil: PostgresUtil = null, metabaseUtil: MetabaseUtil = null): Unit = {
    implicit val pgUtil: PostgresUtil = postgresUtil
    implicit val mUtil: MetabaseUtil = metabaseUtil
    val env = FlinkUtil.getExecutionContext(config)

    // Mentoring
    if (config.config.hasPath("combined.mentoring.dashboard.job.enabled") && config.config.getBoolean("combined.mentoring.dashboard.job.enabled")) {
      val mentoringSource = kafkaConnector.kafkaJobRequestSourceWithProperties[MentoringEvent](config.mentoringInputTopic, config.mentoringKafkaConsumerProperties)
      env.addSource(mentoringSource)
        .name("mentoring-dashboard-consumer").uid("mentoring-dashboard-consumer")
        .setParallelism(config.mentoringConsumerParallelism).rebalance
        .process(new MentoringMetabaseDashboardFunction(config))
        .name("mentoring-dashboard-processor").uid("mentoring-dashboard-processor")
        .setParallelism(config.mentoringProcessParallelism)
    }

    // Observation
    if (config.config.hasPath("combined.observation.dashboard.job.enabled") && config.config.getBoolean("combined.observation.dashboard.job.enabled")) {
      val observationSource = kafkaConnector.kafkaJobRequestSourceWithProperties[ObservationEvent](config.observationInputTopic, config.observationKafkaConsumerProperties)
      env.addSource(observationSource)
        .name("observation-dashboard-consumer").uid("observation-dashboard-consumer")
        .setParallelism(config.observationConsumerParallelism).rebalance
        .process(new ObservationMetabaseDashboardFunction(config))
        .name("observation-dashboard-processor").uid("observation-dashboard-processor")
        .setParallelism(config.observationProcessParallelism)
    }

    // Project
    if (config.config.hasPath("combined.project.dashboard.job.enabled") && config.config.getBoolean("combined.project.dashboard.job.enabled")) {
      val projectSource = kafkaConnector.kafkaJobRequestSourceWithProperties[ProjectEvent](config.projectInputTopic, config.projectKafkaConsumerProperties)
      env.addSource(projectSource)
        .name("project-dashboard-consumer").uid("project-dashboard-consumer")
        .setParallelism(config.projectConsumerParallelism).rebalance
        .process(new ProjectMetabaseDashboardFunction(config))
        .name("project-dashboard-processor").uid("project-dashboard-processor")
        .setParallelism(config.projectProcessParallelism)
    }

    // Survey
    if (config.config.hasPath("combined.survey.dashboard.job.enabled") && config.config.getBoolean("combined.survey.dashboard.job.enabled")) {
      val surveySource = kafkaConnector.kafkaJobRequestSourceWithProperties[SurveyEvent](config.surveyInputTopic, config.surveyKafkaConsumerProperties)
      env.addSource(surveySource)
        .name("survey-dashboard-consumer").uid("survey-dashboard-consumer")
        .setParallelism(config.surveyConsumerParallelism).rebalance
        .process(new SurveyMetabaseDashboardFunction(config))
        .name("survey-dashboard-processor").uid("survey-dashboard-processor")
        .setParallelism(config.surveyProcessParallelism)
    }

    // User
    if (config.config.hasPath("combined.user.dashboard.job.enabled") && config.config.getBoolean("combined.user.dashboard.job.enabled")) {
      val userSource = kafkaConnector.kafkaJobRequestSourceWithProperties[UserEvent](config.userInputTopic, config.userKafkaConsumerProperties)
      env.addSource(userSource)
        .name("user-dashboard-consumer").uid("user-dashboard-consumer")
        .setParallelism(config.userConsumerParallelism).rebalance
        .process(new UserMetabaseDashboardFunction(config))
        .name("user-dashboard-processor").uid("user-dashboard-processor")
        .setParallelism(config.userProcessParallelism)
    }

    // User Service
    if (config.config.hasPath("combined.user.mapping.job.enabled") && config.config.getBoolean("combined.user.mapping.job.enabled")) {
      // Create a UserServiceConfig wrapper for compatibility
      val userServiceSource = kafkaConnector.kafkaJobRequestSourceWithProperties[UserMappingEvent](config.userServiceInputTopic, config.userServiceKafkaConsumerProperties)
      val userServiceStream = env.addSource(userServiceSource)
        .name("user-service-consumer").uid("user-service-consumer")
        .setParallelism(config.userServiceConsumerParallelism).rebalance
        .process(new UserServiceFunction(config))
        .name("user-service-processor").uid("user-service-processor")
        .setParallelism(config.userServiceProcessParallelism)

      // Sink notification events to Kafka
      val notificationSideOutput = userServiceStream.getSideOutput(config.userServiceOutputTag)
      notificationSideOutput
        .addSink(kafkaConnector.kafkaStringSink(config.notificationOutputTopic))
        .name("notification-service-producer").uid("notification-service-producer")
        .setParallelism(config.notificationProducerParallelism)
    }

    // Program Service
    if (config.config.hasPath("combined.program.mapping.job.enabled") && config.config.getBoolean("combined.program.mapping.job.enabled")) {
      val programServiceSource = kafkaConnector.kafkaJobRequestSourceWithProperties[UserMappingEvent](config.programServiceInputTopic, config.programServiceKafkaConsumerProperties)
      env.addSource(programServiceSource)
        .name("program-service-consumer").uid("program-service-consumer")
        .setParallelism(config.programServiceConsumerParallelism).rebalance
        .process(new ProgramServiceFunction(config))
        .name("program-service-processor").uid("program-service-processor")
        .setParallelism(config.programServiceProcessParallelism)
    }

    env.execute("Combined Dashboard Creator Job")
  }
}