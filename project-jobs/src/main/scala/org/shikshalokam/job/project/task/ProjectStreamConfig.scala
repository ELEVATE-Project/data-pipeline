package org.shikshalokam.job.project.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.shikshalokam.job.project.domain.Event
import org.shikshalokam.job.BaseJobConfig


class ProjectStreamConfig(override val config: Config) extends BaseJobConfig(config, "ProjectsStreamJob") {

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // Parallelism
  val mlProjectsParallelism: Int = config.getInt("task.ml.projects.parallelism")

  // Consumers
  val mlProjectsConsumer: String = "ml-project-consumer"

  // Functions
  val projectsStreamFunction: String = "ProjectStreamFunction"

  // User delete job metrics
  val projectsCleanupHit = "project-cleanup-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val totalEventsCount = "total-transfer-events-count"

}