package org.shikshalokam.job.mitra.stream.processor.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.shikshalokam.job.connector.FlinkKafkaConnector
import org.shikshalokam.job.mitra.stream.processor.domain.{DiscussionEvent, StoryEvent}
import org.shikshalokam.job.mitra.stream.processor.functions.{DiscussionStreamFunction, StoryStreamFunction}
import org.shikshalokam.job.util.{FlinkUtil, ScalaJsonUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import java.io.File
import java.util.concurrent.TimeUnit

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

    val streamType = Option(params.get("streamType"))
    runJob(unifiedStreamConfig, kafkaConnector, streamType)
  }

  def runJob(config: MitraStreamConfig, kafkaConnector: FlinkKafkaConnector, streamType: Option[String]): Unit = {
    val env = FlinkUtil.getExecutionContext(config)

    if (streamType.contains("discussion") || (streamType.isEmpty && config.isDiscussionStreamEnabled)) {
      val discussionSource = kafkaConnector.kafkaJobRequestSource[DiscussionEvent](config.discussionInputTopic)
      val parallelism = if (streamType.contains("discussion")) 1 else config.discussionConsumerParallelism

      val inputStream = env.addSource(discussionSource)
        .name("discussion-consumer").uid("discussion-consumer")
        .setParallelism(parallelism).rebalance

      val processedStream = applyDiscussionAsync(inputStream, config)

      processedStream.getSideOutput(config.discussionOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.discussionOutputTopic))
        .name("discussion-sink").setParallelism(config.discussionSinkParallelism)
    }

    if (streamType.contains("story") || (streamType.isEmpty && config.isStoryStreamEnabled)) {
      val storySource = kafkaConnector.kafkaJobRequestSource[StoryEvent](config.storyInputTopic)
      val parallelism = if (streamType.contains("story")) 1 else config.storyConsumerParallelism

      val inputStream = env.addSource(storySource)
        .name("story-consumer").uid("story-consumer")
        .setParallelism(parallelism).rebalance

      val processedStream = applyStoryAsync(inputStream, config)

      // Sink for feed (PII) analysis output
      processedStream.getSideOutput(config.feedOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.storyOutputTopic))
        .name("feed-pii-sink").setParallelism(config.storySinkParallelism)

      // Sink for story ranking output
      processedStream.getSideOutput(config.storyRankingOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.storyOutputTopic))
        .name("story-ranking-sink").setParallelism(config.storySinkParallelism)
    }

    env.execute("Stream Processor Job")
  }

  private def applyDiscussionAsync(inputStream: DataStream[DiscussionEvent], config: MitraStreamConfig): DataStream[DiscussionEvent] = {
    val asyncStream = AsyncDataStream.unorderedWait(
      inputStream,
      new DiscussionStreamFunction(config),
      60000L,
      TimeUnit.MILLISECONDS,
      100
    ).name("discussion-async-processor").uid("discussion-async-processor")

    asyncStream.process(new ProcessFunction[DiscussionEvent, DiscussionEvent] {
      override def processElement(value: DiscussionEvent, ctx: ProcessFunction[DiscussionEvent, DiscussionEvent]#Context, out: Collector[DiscussionEvent]): Unit = {
        val outputData = Map(
          "discussionId" -> value.id,
          "thematic_analysis" -> value.thematicResult
        )
        ctx.output(config.discussionOutputTag, ScalaJsonUtil.serialize(outputData))
        out.collect(value)
      }
    })
  }

  private def applyStoryAsync(inputStream: DataStream[StoryEvent], config: MitraStreamConfig): DataStream[StoryEvent] = {

    val asyncStream = AsyncDataStream.unorderedWait(
      inputStream,
      new StoryStreamFunction(config),
      120000L, // 120 seconds timeout (increased for PDF download and processing)
      TimeUnit.MILLISECONDS,
      10
    ).name("story-async-processor").uid("story-async-processor")

    asyncStream.process(new ProcessFunction[StoryEvent, StoryEvent] {
      override def processElement(value: StoryEvent, ctx: ProcessFunction[StoryEvent, StoryEvent]#Context, out: Collector[StoryEvent]): Unit = {
        val feedOutputData = Map(
          "storyId" -> value.id,
          "title" -> value.title,
          "pii_analysis_status" -> "completed",
          "processing_type" -> "pii_analysis"
        )
        ctx.output(config.feedOutputTag, ScalaJsonUtil.serialize(feedOutputData))

        val storyOutputData = Map(
          "storyId" -> value.id,
          "title" -> value.title,
          "ranking_status" -> "completed",
          "processing_type" -> "story_ranking"
        )
        ctx.output(config.storyRankingOutputTag, ScalaJsonUtil.serialize(storyOutputData))
        out.collect(value)
      }
    }).name("story-output-processor").uid("story-output-processor")
  }

}