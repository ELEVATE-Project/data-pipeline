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
  val feedOutputTag = new OutputTag[String]("feed-output-events")
  val storyRankingOutputTag = new OutputTag[String]("story-output-events")
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

  // PostgreSQL connection config
  val pgHost: String = config.getString("postgres.host")
  val pgPort: String = config.getString("postgres.port")
  val pgUsername: String = config.getString("postgres.username")
  val pgPassword: String = config.getString("postgres.password")
  val pgDataBase: String = config.getString("postgres.database")
  val promptsTable: String = config.getString("postgres.tables.promptsTable")
  val promptsVersionsTable: String = config.getString("postgres.tables.promptsVersionsTable")
  val themesTable: String = config.getString("postgres.tables.themesTable")
  val discussionsTable: String = config.getString("postgres.tables.discussionsTable")
  val voicesTable: String = config.getString("postgres.tables.voicesTable")
  val storiesMetaTable: String = config.getString("postgres.tables.storiesMetaTable")
  val storiesTable: String = config.getString("postgres.tables.storiesTable")
  val feedsTable: String = config.getString("postgres.tables.feedsTable")

  //Ai model config
  val modelProvider = config.getString("ai.model.provider")
  val bedrockModelVesrion = config.getString("bedrock.model.version")
  val bedrockModelId = config.getString("bedrock.model.id")
  val bedrockModelTemperature = config.getDouble("bedrock.model.temperature")
  val bedrockMaxTokens = config.getInt("bedrock.model.max.tokens")

  val geminiApiKey = config.getString("gemini.api.key")
  val geminiModelName = config.getString("gemini.model.name")
  val geminiMaxTokens = config.getInt("gemini.model.max.tokens")
  val geminiTemperature = config.getDouble("gemini.model.temperature")

  // AWS Configuration
  val awsRegion = config.getString("aws.region")
  val awsAccessKey = config.getString("aws.access.key")
  val awsSecretKey = config.getString("aws.secret.key")

  // Sql queries
  val thematicAnalyzerQuery: String = s"SELECT pv.content FROM $promptsTable p JOIN $promptsVersionsTable pv ON pv.id = p.current_version_id WHERE p.name = 'Thematic Analyzer' LIMIT 1;"
  val piiAnalyzerQuery: String = s"SELECT pv.content FROM $promptsTable p JOIN $promptsVersionsTable pv ON pv.id = p.current_version_id WHERE p.name = 'Pii Analyzer' LIMIT 1;"
  val storyAnalyzerQuery: String = s"SELECT pv.content FROM $promptsTable p JOIN $promptsVersionsTable pv ON pv.id = p.current_version_id WHERE p.name = 'Story Analyzer' LIMIT 1;"

  // === Cloud Storage Configuration ===
  val cloudStorageProvider: String = config.getString("cloud.storage.provider") // "s3", "gcp", "oracle"
  val cloudBucketName: String = config.getString("cloud.storage.bucket.name")
  val cloudRegion: String = config.getString("cloud.storage.region")
  val makePublic: Boolean = if (config.hasPath("cloud.storage.make.public")) {
    config.getBoolean("cloud.storage.make.public")
  } else true

  // === GCP Specific Configuration ===
  val gcpProjectId: String = if (config.hasPath("gcp.project.id")) {
    config.getString("gcp.project.id")
  } else ""

  val gcpCredentialsPath: String = if (config.hasPath("gcp.credentials.path")) {
    config.getString("gcp.credentials.path")
  } else ""

  // === S3 Specific Configuration (for backward compatibility) ===
  val s3BucketName: String = cloudBucketName
  val s3Region: String = cloudRegion

  val baseUrl: String = config.getString("base.url")



}