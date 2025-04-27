package org.shikshalokam.job.survey.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.survey.stream.processor.domain.Event
import org.shikshalokam.job.survey.stream.processor.task.SurveyStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.util
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.immutable._

class SurveyStreamFunction(config: SurveyStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[SurveyStreamFunction])

  override def metricsList(): List[String] = {
    List(config.surveysCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
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

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    {
      println(s"***************** Start of Processing the Survey Event with Id = ${event._id} *****************")


      // Uncomment the bellow lines to create table schema for the first time.
      postgresUtil.createTable(config.createSolutionsTable, config.solutions)
      postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboard_metadata)


      def pushSurveyDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context): util.HashMap[String, AnyRef] = {
        val objects = new util.HashMap[String, AnyRef]() {
          put("_id", java.util.UUID.randomUUID().toString)
          put("reportType", "Survey")
          put("publishedAt", DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault())
            .format(Instant.ofEpochMilli(System.currentTimeMillis())).asInstanceOf[AnyRef])
          put("dashboardData", dashboardData)
        }
        val event = ScalaJsonUtil.serialize(objects)
        context.output(config.eventOutputTag, event)
        println(s"----> Pushed new Kafka message to ${config.outputTopic} topic")
        println(objects)
        objects
      }

    }
  }
}