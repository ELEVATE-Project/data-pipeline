package org.shikshalokam.job.mitra.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.mitra.stream.processor.domain.DiscussionEvent
import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util
import scala.collection.immutable._

class DiscussionStreamFunction(config: MitraStreamConfig)(implicit val mapTypeInfo: TypeInformation[DiscussionEvent], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[DiscussionEvent, DiscussionEvent](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DiscussionStreamFunction])

  override def metricsList(): List[String] = {
    List(config.discussionCleanupHit, config.discussionSkipCount, config.discussionSuccessCount, config.discussionTotalEventsCount)
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

  override def processElement(event: DiscussionEvent, context: ProcessFunction[DiscussionEvent, DiscussionEvent]#Context, metrics: Metrics): Unit = {
    logger.info(s"***************** Start of Processing the Discussion Event with Id = ${event._id} *****************")
    println(s"***************** Start of Processing the Discussion Event with Id = ${event._id} *****************")
    println(event._id)

    // Always increment total events
    metrics.incCounter(config.discussionTotalEventsCount)

    if (2+2==4) {
      metrics.incCounter(config.discussionCleanupHit)
    } else if (2-2==5) {
      metrics.incCounter(config.discussionSkipCount)
    } else {
      metrics.incCounter(config.discussionSuccessCount)
    }

    println(
      s"""
         |Final Discussion Metrics:
         |total   = ${metrics.get(config.discussionTotalEventsCount)}
         |success = ${metrics.get(config.discussionSuccessCount)}
         |skip    = ${metrics.get(config.discussionSkipCount)}
         |cleanup = ${metrics.get(config.discussionCleanupHit)}
     """.stripMargin
    )

  }


}

