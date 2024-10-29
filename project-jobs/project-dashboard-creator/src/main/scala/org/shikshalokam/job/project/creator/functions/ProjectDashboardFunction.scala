package org.shikshalokam.job.project.creator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.BaseProcessFunction
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.project.creator.domain.Event
import org.shikshalokam.job.project.creator.task.ProjectDashboardConfig
import org.shikshalokam.job.util.PostgresUtil
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.immutable._

class ProjectDashboardFunction(config: ProjectDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProjectDashboardFunction])

  override def metricsList(): List[String] = {
    List(config.projectsCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
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
    println(s"***************** Start of Processing the Project Event with Id = ${event._id}*****************")

    println(s"***************** End of Processing the Project Event *****************\n")
  }
}
