package org.shikshalokam.job.mitra.stream.processor.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.mitra.stream.processor.domain.{DiscussionEvent, StoryEvent}
import org.shikshalokam.job.mitra.stream.processor.functions.{DiscussionStreamFunction, StoryStreamFunction}
import org.shikshalokam.job.util.FlinkUtil

import java.io.File

object MitraStreamTask {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val config: Config = if (params.has("config.file.path")) {
      ConfigFactory.parseFile(new File(params.get("config.file.path"))).resolve()
    } else {
      ConfigFactory.load("mitra-stream.conf").resolve()
    }

    val unifiedStreamConfig: MitraStreamConfig = new MitraStreamConfig(config)
    val kafkaConnector: FlinkKafkaConnector = new FlinkKafkaConnector(unifiedStreamConfig)

    val streamType = params.get("streamType")
    if (streamType != null && streamType.nonEmpty) {
      runJob(unifiedStreamConfig, kafkaConnector, streamType)
    } else {
      runJob(unifiedStreamConfig, kafkaConnector)
    }
  }

  def runJob(config: MitraStreamConfig, kafkaConnector: FlinkKafkaConnector): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    if (config.isDiscussionStreamEnabled) {
      val discussionSource = kafkaConnector.kafkaJobRequestSource[DiscussionEvent](config.discussionInputTopic)
      val discussionStream = env.addSource(discussionSource)
        .name("discussion-consumer").uid("discussion-consumer")
        .setParallelism(config.discussionConsumerParallelism).rebalance
        .process(new DiscussionStreamFunction(config))
        .name("discussion-processor").uid("discussion-processor")
        .setParallelism(config.discussionProcessParallelism)

      discussionStream.getSideOutput(config.discussionOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.discussionOutputTopic))
        .name("discussion-sink").setParallelism(1)
    }

    if (config.isStoryStreamEnabled) {
      val storySource = kafkaConnector.kafkaJobRequestSource[StoryEvent](config.storyInputTopic)
      val storyStream = env.addSource(storySource)
        .name("story-consumer").uid("story-consumer")
        .setParallelism(config.storyConsumerParallelism).rebalance
        .process(new StoryStreamFunction(config))
        .name("story-processor").uid("story-processor")
        .setParallelism(config.storyProcessParallelism)

      storyStream.getSideOutput(config.storyOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.storyOutputTopic))
        .name("story-sink").setParallelism(config.storySinkParallelism)
    }


    env.execute("Stream Processor Job")
  }

  def runJob(config: MitraStreamConfig, kafkaConnector: FlinkKafkaConnector, streamType: String): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    streamType match {
      case "discussion" =>
        val discussionSource = kafkaConnector.kafkaJobRequestSource[DiscussionEvent](config.discussionInputTopic)
        val discussionStream = env.addSource(discussionSource)
          .name("discussion-consumer").uid("discussion-consumer")
          .setParallelism(1).rebalance
          .process(new DiscussionStreamFunction(config))
          .name("discussion-processor").uid("discussion-processor")
          .setParallelism(1)

        discussionStream.getSideOutput(config.discussionOutputTag)
          .addSink(kafkaConnector.kafkaStringSink(config.discussionOutputTopic))
          .name("discussion-sink").setParallelism(1)

      case "story" =>
        val storySource = kafkaConnector.kafkaJobRequestSource[StoryEvent](config.storyInputTopic)
        val storyStream = env.addSource(storySource)
          .name("story-consumer").uid("story-consumer")
          .setParallelism(config.storyConsumerParallelism).rebalance
          .process(new StoryStreamFunction(config))
          .name("story-processor").uid("story-processor")
          .setParallelism(config.storyProcessParallelism)

        storyStream.getSideOutput(config.storyOutputTag)
          .addSink(kafkaConnector.kafkaStringSink(config.storyOutputTopic))
          .name("test-story-sink").setParallelism(config.storySinkParallelism)

      case _ =>
        runJob(config, kafkaConnector)
    }

    env.execute("Stream Processor Job")
  }
}