package org.shikshalokam.job.mitra.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.mitra.stream.processor.domain.StoryEvent
import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util
import scala.collection.immutable._


class StoryStreamFunction(config: MitraStreamConfig)(implicit val mapTypeInfo: TypeInformation[StoryEvent], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[StoryEvent, StoryEvent](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[StoryStreamFunction])

  override def metricsList(): List[String] = {
    List(config.storyCleanupHit, config.storySkipCount, config.storySuccessCount, config.storyTotalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: StoryEvent, context: ProcessFunction[StoryEvent, StoryEvent]#Context, metrics: Metrics): Unit = {
    logger.info(s"***************** Start of Processing the Story StoryEvent with Id = ${event._id} *****************")
    println(s"***************** Start of Processing the Story StoryEvent with Id = ${event._id} *****************")
    println(event._id)
  }

}