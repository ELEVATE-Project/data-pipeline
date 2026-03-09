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
    val baseConfig: Config = if (params.has("config.content")) {
      val decoded = new String(java.util.Base64.getDecoder.decode(params.get("config.content")))
      ConfigFactory.parseString(decoded).resolve()
    } else if (params.has("config.file.path")) {
      ConfigFactory.parseFile(new File(params.get("config.file.path"))).resolve()
    } else {
      ConfigFactory.parseFile(new java.io.File("unified-pipeline.conf")).resolve()
    }

    val config = baseConfig.getConfig("stream").withFallback(baseConfig)
    val unifiedStreamConfig: UnifiedStreamConfig = new UnifiedStreamConfig(config)
    val kafkaConnector: FlinkKafkaConnector = new FlinkKafkaConnector(unifiedStreamConfig)

    val streamType = params.get("streamType")
    if (streamType != null && streamType.nonEmpty) {
      runJob(unifiedStreamConfig, kafkaConnector, streamType)
    } else {
      runJob(unifiedStreamConfig, kafkaConnector)
    }
  }

  def runJob(config: UnifiedStreamConfig, kafkaConnector: FlinkKafkaConnector): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    if (config.isProjectStreamEnabled) {
      val projectSource = kafkaConnector.kafkaJobRequestSource[ProjectEvent](config.projectInputTopic)
      val projectStream = env.addSource(projectSource)
        .name("project-consumer").uid("project-consumer")
        .setParallelism(config.projectConsumerParallelism).rebalance
        .process(new ProjectStreamFunction(config))
        .name("project-processor").uid("project-processor")
        .setParallelism(config.projectProcessParallelism)

      projectStream.getSideOutput(config.projectOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.projectOutputTopic))
        .name("project-sink").setParallelism(1)
    }

    if (config.isSurveyStreamEnabled) {
      val surveySource = kafkaConnector.kafkaJobRequestSource[SurveyEvent](config.surveyInputTopic)
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
      val observationSource = kafkaConnector.kafkaJobRequestSource[ObservationEvent](config.observationInputTopic)
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
      val userSource = kafkaConnector.kafkaJobRequestSource[UserEvent](config.userInputTopic)
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
      val mentoringSource = kafkaConnector.kafkaJobRequestSource[MentoringEvent](config.mentoringInputTopic)
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

    env.execute("Stream Processor Job")
  }

  def runJob(config: UnifiedStreamConfig, kafkaConnector: FlinkKafkaConnector, streamType: String): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    streamType match {
      case "project" =>
        val projectSource = kafkaConnector.kafkaJobRequestSource[ProjectEvent](config.projectInputTopic)
        val projectStream = env.addSource(projectSource)
          .name("project-consumer").uid("project-consumer")
          .setParallelism(1).rebalance
          .process(new ProjectStreamFunction(config))
          .name("project-processor").uid("project-processor")
          .setParallelism(1)

        projectStream.getSideOutput(config.projectOutputTag)
          .addSink(kafkaConnector.kafkaStringSink(config.projectOutputTopic))
          .name("project-sink").setParallelism(1)

      case "survey" =>
        val surveySource = kafkaConnector.kafkaJobRequestSource[SurveyEvent](config.surveyInputTopic)
        val surveyStream = env.addSource(surveySource)
          .name("survey-consumer").uid("survey-consumer")
          .setParallelism(config.surveyConsumerParallelism).rebalance
          .process(new SurveyStreamFunction(config))
          .name("survey-processor").uid("survey-processor")
          .setParallelism(config.surveyProcessParallelism)

        surveyStream.getSideOutput(config.surveyOutputTag)
          .addSink(kafkaConnector.kafkaStringSink(config.surveyOutputTopic))
          .name("test-survey-sink").setParallelism(config.surveySinkParallelism)

      case "observation" =>
        val observationSource = kafkaConnector.kafkaJobRequestSource[ObservationEvent](config.observationInputTopic)
        val observationStream = env.addSource(observationSource)
          .name("observation-consumer").uid("observation-consumer")
          .setParallelism(config.observationConsumerParallelism).rebalance
          .process(new ObservationStreamFunction(config))
          .name("observation-processor").uid("observation-processor")
          .setParallelism(config.observationProcessParallelism)

        observationStream.getSideOutput(config.eventOutputTag)
          .addSink(kafkaConnector.kafkaStringSink(config.observationOutputTopic))
          .name("test-observation-sink").setParallelism(config.observationSinkParallelism)

      case "user" =>
        val userSource = kafkaConnector.kafkaJobRequestSource[UserEvent](config.userInputTopic)
        val userStream = env.addSource(userSource)
          .name("user-consumer").uid("user-consumer")
          .setParallelism(config.userConsumerParallelism).rebalance
          .process(new UserStreamFunction(config))
          .name("user-processor").uid("user-processor")
          .setParallelism(config.userProcessParallelism)

        userStream.getSideOutput(config.eventOutputTag) // Note: User stream uses eventOutputTag for user dashboard
          .addSink(kafkaConnector.kafkaStringSink(config.userOutputTopic))
          .name("user-sink").setParallelism(config.userSinkParallelism)

      case "mentoring" =>
        val mentoringSource = kafkaConnector.kafkaJobRequestSource[MentoringEvent](config.mentoringInputTopic)
        val mentoringStream = env.addSource(mentoringSource)
          .name("mentoring-consumer").uid("mentoring-consumer")
          .setParallelism(config.mentoringConsumerParallelism).rebalance
          .process(new MentoringStreamFunction(config))
          .name("mentoring-processor").uid("mentoring-processor")
          .setParallelism(config.mentoringProcessParallelism)

        mentoringStream.getSideOutput(config.mentoringEventOutputTag)
          .addSink(kafkaConnector.kafkaStringSink(config.mentoringOutputTopic))
          .name("mentoring-sink").setParallelism(config.mentoringSinkParallelism)

      case _ =>
        logger.warn(s"Unknown streamType '$streamType', falling back to all-streams mode.")
        runJob(config, kafkaConnector)
        return  // runJob already calls env.execute
    }

    env.execute("Stream Processor Job")
  }
}