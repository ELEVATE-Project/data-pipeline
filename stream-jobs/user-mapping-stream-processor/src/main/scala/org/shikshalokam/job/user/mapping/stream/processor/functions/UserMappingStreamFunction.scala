package org.shikshalokam.job.user.mapping.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.mapping.stream.processor.domain.ObservationEvent
import org.shikshalokam.job.user.mapping.stream.processor.task.UserMappingStreamConfig
import org.shikshalokam.job.user.mapping.stream.processor.util.{FieldMapper, UserApiClient}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class UserMappingStreamFunction(config: UserMappingStreamConfig)(implicit val mapTypeInfo: TypeInformation[ObservationEvent])
  extends BaseProcessFunction[ObservationEvent, ObservationEvent](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserMappingStreamFunction])

  override def metricsList(): List[String] = {
    List(config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println("[UserMappingStreamFunction] Initializing...")
    println("[UserMappingStreamFunction] FieldMapper mappings loaded")
    FieldMapper.getMappings.foreach { case (source, target) =>
      println(s"[UserMappingStreamFunction] Mapping: $source -> $target")
    }
  }

  override def close(): Unit = {
    super.close()
    println("[UserMappingStreamFunction] Closing...")
  }

  override def processElement(event: ObservationEvent, context: ProcessFunction[ObservationEvent, ObservationEvent]#Context, metrics: Metrics): Unit = {
    
    try {
      println(s"***************** Start Processing Observation Event *****************")
      println(s"[UserMappingStreamFunction] Event Type: ${event.eventType}")
      println(s"[UserMappingStreamFunction] Student ID: ${event.studentId}")
      println(s"[UserMappingStreamFunction] Organization ID: ${event.organizationId}")
      
      // Update total events count metric
      metrics.incCounter(config.totalEventsCount)
      
      // Validate event
      val studentId = event.studentId
      if (studentId == null || studentId.trim.isEmpty) {
        println(s"[UserMappingStreamFunction] ERROR: studentId/id is null or empty, skipping event")
        logger.error("[UserMappingStreamFunction] studentId/id is null or empty")
        metrics.incCounter(config.skipCount)
        return
      }
      
      if (event.observationData == null || event.observationData.isEmpty) {
        println(s"[UserMappingStreamFunction] WARNING: observationData is null or empty for studentId=$studentId")
        logger.warn(s"[UserMappingStreamFunction] observationData is null or empty for studentId=$studentId")
        metrics.incCounter(config.skipCount)
        return
      }
      
      // Extract observation data
      val observationData = event.observationData
      println(s"[UserMappingStreamFunction] Observation data keys: ${observationData.keySet().asScala.mkString(", ")}")
      
      // Transform observation data to profile format using FieldMapper
      println(s"[UserMappingStreamFunction] Transforming observation data to profile format...")
      val profileData = FieldMapper.transform(observationData)
      
      // Check if profile data is empty
      val profileObj = profileData.value.get("profile")
      if (profileObj.isEmpty || profileObj.get.asInstanceOf[ujson.Obj].value.isEmpty) {
        println(s"[UserMappingStreamFunction] WARNING: No profile data to update after transformation for studentId=$studentId")
        logger.warn(s"[UserMappingStreamFunction] No profile data to update after transformation for studentId=$studentId")
        metrics.incCounter(config.skipCount)
        return
      }
      
      // Call User Service API to patch the profile
      println(s"[UserMappingStreamFunction] Calling UserApiClient.patchProfile for studentId=$studentId...")
      UserApiClient.patchProfile(studentId, profileData) match {
        case Success(true) =>
          println(s"[UserMappingStreamFunction] SUCCESS: Profile updated for studentId=$studentId")
          logger.info(s"[UserMappingStreamFunction] Successfully updated profile for studentId=$studentId")
          metrics.incCounter(config.successCount)
          
        case Failure(exception) =>
          println(s"[UserMappingStreamFunction] FAILED: Could not update profile for studentId=$studentId: ${exception.getMessage}")
          logger.error(s"[UserMappingStreamFunction] Failed to update profile for studentId=$studentId", exception)
          metrics.incCounter(config.skipCount)
          // Re-throw to trigger Flink retry mechanism if configured
          throw new Exception(s"Failed to update profile for studentId=$studentId", exception)
      }
      
      println(s"***************** Completed Processing Observation Event for studentId=$studentId *****************")
      
    } catch {
      case e: Exception =>
        println(s"[UserMappingStreamFunction] EXCEPTION: Error processing observation event: ${e.getMessage}")
        logger.error(s"[UserMappingStreamFunction] Exception processing observation event", e)
        e.printStackTrace()
        metrics.incCounter(config.skipCount)
        // Re-throw to trigger Flink retry mechanism if configured
        throw e
    }
  }

}
