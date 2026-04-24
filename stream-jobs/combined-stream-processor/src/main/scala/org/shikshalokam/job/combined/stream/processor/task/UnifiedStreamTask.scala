package org.shikshalokam.job.combined.stream.processor.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.util.FlinkUtil
import org.slf4j.LoggerFactory


import org.shikshalokam.job.combined.stream.processor.domain.{MentoringEvent, ObservationEvent, ProjectEvent, SurveyEvent, UserEvent}
import org.shikshalokam.job.combined.stream.processor.functions.{MentoringStreamFunction, ObservationStreamFunction, ProjectStreamFunction, SurveyStreamFunction, UserStreamFunction}

import java.io.File

object UnifiedStreamTask {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val baseConfig: Config = if (params.has("config.file.path")) {
      ConfigFactory.parseFile(new File(params.get("config.file.path"))).resolve()
    } else {
      ConfigFactory.parseFile(new java.io.File("unified-common.conf")).resolve()
    }

    val unifiedStreamConfig: UnifiedStreamConfig = new UnifiedStreamConfig(baseConfig)
    val kafkaConnector: FlinkKafkaConnector = new FlinkKafkaConnector(unifiedStreamConfig)
    runJob(unifiedStreamConfig, kafkaConnector)
  }

  def runJob(config: UnifiedStreamConfig, kafkaConnector: FlinkKafkaConnector): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    if (config.isProjectStreamEnabled) {
      val projectSource = kafkaConnector.kafkaJobRequestSourceWithProperties[ProjectEvent](config.projectInputTopic, config.projectKafkaConsumerProperties)
      val projectStream = env.addSource(projectSource)
        .name("project-consumer").uid("project-consumer")
        .setParallelism(config.projectConsumerParallelism).rebalance
        .process(new ProjectStreamFunction(config))
        .name("project-processor").uid("project-processor")
        .setParallelism(config.projectProcessParallelism)

      projectStream.getSideOutput(config.projectOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.projectOutputTopic))
        .name("project-sink").setParallelism(config.projectSinkParallelism)
    }

    if (config.isSurveyStreamEnabled) {
      val surveySource = kafkaConnector.kafkaJobRequestSourceWithProperties[SurveyEvent](config.surveyInputTopic, config.surveyKafkaConsumerProperties)
      val surveyStream = env.addSource(surveySource)
        .name("survey-consumer").uid("survey-consumer")
        .setParallelism(config.surveyConsumerParallelism).rebalance
        .process(new SurveyStreamFunction(config))
        .name("survey-processor").uid("survey-processor")
        .setParallelism(config.surveyProcessParallelism)

      surveyStream.getSideOutput(config.surveyOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.surveyOutputTopic))
        .name("survey-sink").setParallelism(config.surveySinkParallelism)
    }

    if (config.isObservationStreamEnabled) {
      val observationSource = kafkaConnector.kafkaJobRequestSourceWithProperties[ObservationEvent](config.observationInputTopic, config.observationKafkaConsumerProperties)
      val observationStream = env.addSource(observationSource)
        .name("observation-consumer").uid("observation-consumer")
        .setParallelism(config.observationConsumerParallelism).rebalance
        .process(new ObservationStreamFunction(config))
        .name("observation-processor").uid("observation-processor")
        .setParallelism(config.observationProcessParallelism)

      observationStream.getSideOutput(config.eventOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.observationOutputTopic))
        .name("observation-sink").setParallelism(config.observationSinkParallelism)
    }

    if (config.isUserStreamEnabled) {
      val userSource = kafkaConnector.kafkaJobRequestSourceWithProperties[UserEvent](config.userInputTopic, config.userKafkaConsumerProperties)
      val userStream = env.addSource(userSource)
        .name("user-consumer").uid("user-consumer")
        .setParallelism(config.userConsumerParallelism).rebalance
        .process(new UserStreamFunction(config))
        .name("user-processor").uid("user-processor")
        .setParallelism(config.userProcessParallelism)

      userStream.getSideOutput(config.userOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.userOutputTopic))
        .name("user-sink").setParallelism(config.userSinkParallelism)
    }

    if (config.isMentoringStreamEnabled) {
      val mentoringSource = kafkaConnector.kafkaJobRequestSourceWithProperties[MentoringEvent](config.mentoringInputTopic, config.mentoringKafkaConsumerProperties)
      val mentoringStream = env.addSource(mentoringSource)
        .name("mentoring-consumer").uid("mentoring-consumer")
        .setParallelism(config.mentoringConsumerParallelism).rebalance
        .process(new MentoringStreamFunction(config))
        .name("mentoring-processor").uid("mentoring-processor")
        .setParallelism(config.mentoringProcessParallelism)

      mentoringStream.getSideOutput(config.mentoringEventOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.mentoringOutputTopic))
        .name("mentoring-sink").setParallelism(config.mentoringSinkParallelism)
    }

    env.execute("Combined Stream Processor Job")
  }

}