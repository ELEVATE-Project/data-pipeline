package org.shikshalokam.job.mitra.stream.processor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.shikshalokam.job.BaseJobConfig
import org.shikshalokam.job.mitra.stream.processor.domain.DiscussionEvent

import scala.collection.JavaConverters._

class MitraStreamConfig(override val config: Config) extends BaseJobConfig(config, "StreamProcessorJob") {

  implicit val mapTypeInfo: TypeInformation[DiscussionEvent] = TypeExtractor.getForClass(classOf[DiscussionEvent])

  // === Discussion ===
  val discussionInputTopic: String = config.getString("kafka.input.discussion.topic")
  val discussionOutputTopic: String = config.getString("kafka.output.discussion.topic")
  val discussionConsumerGroup: String = config.getString("kafka.consumer.discussion.group")
  val discussionConsumerParallelism: Int = config.getInt("task.discussion.consumer.parallelism")
  val discussionProcessParallelism: Int = config.getInt("task.discussion.process.parallelism")
  val discussionSinkParallelism: Int = config.getInt("task.discussion.sink.parallelism")
  val discussionOutputTag = new OutputTag[String]("discussion-dashboard-events")
  val isDiscussionStreamEnabled: Boolean = if (config.hasPath("kafka.input.discussion.enabled")) config.getBoolean("kafka.input.discussion.enabled") else false

  // === Story ===
  val storyInputTopic: String = config.getString("kafka.input.story.topic")
  val storyOutputTopic: String = config.getString("kafka.output.story.topic")
  val storyConsumerGroup: String = config.getString("kafka.consumer.story.group")
  val storyConsumerParallelism: Int = config.getInt("task.story.consumer.parallelism")
  val storyProcessParallelism: Int = config.getInt("task.story.process.parallelism")
  val storySinkParallelism: Int = config.getInt("task.story.sink.parallelism")
  val storyOutputTag = new OutputTag[String]("story-dashboard-events")
  val isTestMode: Boolean = config.hasPath("test.mode") && config.getBoolean("test.mode")
  val isStoryStreamEnabled: Boolean = if (config.hasPath("kafka.input.story.enabled")) config.getBoolean("kafka.input.story.enabled") else false

  // Output Tags
  val discussionEventOutputTag: OutputTag[String] = OutputTag[String]("discussion-dashboard-output-event")
  val storyEventOutputTag: OutputTag[String] = OutputTag[String]("story-dashboard-output-event")

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Discussion submissions job metrics
  val discussionCleanupHit = "discussion-cleanup-hit"
  val discussionSkipCount = "discussion-skipped-message-count"
  val discussionSuccessCount = "discussion-success-message-count"
  val discussionTotalEventsCount = "total-discussion-events-count"

  // Story submissions job metrics
  val storyCleanupHit = "story-cleanup-hit"
  val storySkipCount = "story-skipped-message-count"
  val storySuccessCount = "story-success-message-count"
  val storyTotalEventsCount = "total-story-events-count"

  //report-config
  val reportsEnabled: Set[String] = config.getStringList("reports.enabled").asScala.toSet

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")

}