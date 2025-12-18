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
      val parallelism = config.storyConsumerParallelism

      env.addSource(storySource)
        .name("story-consumer").uid("story-consumer")
        .setParallelism(parallelism).rebalance
        .process(new StoryStreamFunction(config))
        .getSideOutput(config.storyOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.storyOutputTopic))
        .name("story-sink").setParallelism(config.storySinkParallelism)
    }

    env.execute("Stream Processor Job")
  }

  private def applyDiscussionAsync(inputStream: DataStream[DiscussionEvent], config: MitraStreamConfig): DataStream[DiscussionEvent] = {
    val asyncStream = AsyncDataStream.unorderedWait(
      inputStream,
      new DiscussionStreamFunction(config),
      30000L,
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
}