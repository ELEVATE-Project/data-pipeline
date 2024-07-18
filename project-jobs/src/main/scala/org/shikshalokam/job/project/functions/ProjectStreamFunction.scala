package org.shikshalokam.job.project.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.project.domain.Event
import org.shikshalokam.job.project.task.ProjectStreamConfig
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.immutable._


class ProjectStreamFunction(config: ProjectStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event]/**, @transient var mongoUtil: MongoUtil = null*/)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProjectStreamFunction])

  override def metricsList(): List[String] = {
    List(config.projectsCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
//    mongoUtil = new MongoUtil(config.dbHost, config.dbPort, config.dataBase)
  }

  override def close(): Unit = {
//    mongoUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

//    println(s"Processing Project Submission Event")

    }

  }