package org.shikshalokam.job.user.mapping.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.user.mapping.stream.processor.domain.ObservationEvent
import org.shikshalokam.job.user.mapping.stream.processor.task.UserMappingStreamConfig
import org.shikshalokam.job.user.mapping.stream.processor.util.{FieldMapper, UserApiClient}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory
import ujson.Obj

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
    FieldMapper.getMappings.foreach { case (source, target) =>
      println(s"[UserMappingStreamFunction] Mapping: $source -> $target")
    }
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * Recursively print all fields and values from a map structure
   */
  private def printAllFields(map: java.util.Map[String, Any], prefix: String = "", depth: Int = 0, maxDepth: Int = 5): Unit = {
    if (depth > maxDepth) {
      val fullKey = if (prefix.isEmpty) "[Max depth reached]" else s"$prefix[Max depth reached, truncating...]"
      println(s"[UserMappingStreamFunction] $fullKey")
      return
    }
    
    if (map == null || map.isEmpty) {
      val fullKey = if (prefix.isEmpty) "[Empty map]" else s"$prefix = [Empty map]"
      println(s"[UserMappingStreamFunction] $fullKey")
      return
    }
    
    map.asScala.foreach { case (key, value) =>
      val fullKey = if (prefix.isEmpty) key else s"$prefix.$key"
      value match {
        case nestedMap: java.util.Map[_, _] =>
          println(s"[UserMappingStreamFunction] $fullKey = [Map with ${nestedMap.size()} entries]")
          printAllFields(nestedMap.asInstanceOf[java.util.Map[String, Any]], fullKey, depth + 1, maxDepth)
        case list: java.util.List[_] =>
          println(s"[UserMappingStreamFunction] $fullKey = [List with ${list.size()} items]")
          list.asScala.zipWithIndex.foreach { case (item, idx) =>
            item match {
              case itemMap: java.util.Map[_, _] =>
                println(s"[UserMappingStreamFunction] $fullKey[$idx] = [Map]")
                printAllFields(itemMap.asInstanceOf[java.util.Map[String, Any]], s"$fullKey[$idx]", depth + 1, maxDepth)
              case _ =>
                println(s"[UserMappingStreamFunction] $fullKey[$idx] = $item")
            }
          }
        case _ =>
          val valueStr = if (value != null) value.toString else "null"
          val truncatedValue = if (valueStr.length > 200) valueStr.substring(0, 200) + "..." else valueStr
          println(s"[UserMappingStreamFunction] $fullKey = $truncatedValue")
      }
    }
  }

  override def processElement(event: ObservationEvent, context: ProcessFunction[ObservationEvent, ObservationEvent]#Context, metrics: Metrics): Unit = {
    
    try {
      println(s"***************** Start Processing Observation Event *****************")
      
      // Print all fields from the event map
      val eventMap = event.getMap()
      // printAllFields(eventMap)
      
      // Update total events count metric
      metrics.incCounter(config.totalEventsCount)
      
      // Validate event - ensure id is present
      val id = event.id
      if (id == null || id.trim.isEmpty) {
        logger.error("[UserMappingStreamFunction] id is null or empty")
        metrics.incCounter(config.skipCount)
        return
      }
      
      // Extract userProfile from observation event
      // The observationData method already extracts userProfile from the event
      val userProfile = event.observationData
      
      if (userProfile == null || userProfile.isEmpty) {
        logger.warn(s"[UserMappingStreamFunction] userProfile is null or empty for id=$id")
        metrics.incCounter(config.skipCount)
        return
      }
            
      // Transform userProfile data to profile format using FieldMapper
      // FieldMapper will:
      // 1. Only map allowed fields (name, username, dob, phoneCode, about, preferredLanguage, tenantCode, meta)
      // 2. Skip fields that are null, empty, or missing
      // 3. Return a profile object with only non-empty fields
      val profileData = FieldMapper.transform(userProfile)
      
      // Check if profile data contains any valid (non-empty) fields
      // Only call API if at least one field is eligible for update
      val profileObj = profileData.value.get("profile")
      if (profileObj.isEmpty) {
        logger.warn(s"[UserMappingStreamFunction] No profile object in transformed data for id=$id")
        metrics.incCounter(config.skipCount)
        return
      }
      
      val profileFields = profileObj.get.asInstanceOf[ujson.Obj].value
      if (profileFields.isEmpty) {
        logger.info(s"[UserMappingStreamFunction] No valid fields to update for id=$id - all fields were empty/null/missing")
        metrics.incCounter(config.skipCount)
        return
      }
            
      // Call User Service API to patch the profile
      // Only non-empty fields will be included in the update request
      println(s"[UserMappingStreamFunction] Calling UserApiClient.patchProfile for id=$id...")
      UserApiClient.patchProfile(id, profileData, config.userServiceBaseUrl, config.userServiceAuthToken) match {
        case Success(true) =>
          logger.info(s"[UserMappingStreamFunction] Successfully updated profile for id=$id with fields: ${profileFields.keys.mkString(", ")}")
          metrics.incCounter(config.successCount)
          
        case Success(false) =>
          logger.warn(s"[UserMappingStreamFunction] API returned success=false for id=$id")
          metrics.incCounter(config.skipCount)

        case Failure(exception) =>
          logger.error(s"[UserMappingStreamFunction] Failed to update profile for id=$id", exception)
          metrics.incCounter(config.skipCount)
          // Log error but continue processing - don't crash the task for API failures
          // The error is already logged and metrics are updated
      }
      
      println(s"***************** Completed Processing Observation Event for id=$id *****************")
      
    } catch {
      case e: Exception =>
        logger.error(s"[UserMappingStreamFunction] Exception processing observation event", e)
        e.printStackTrace()
        metrics.incCounter(config.skipCount)
        // Log error but continue processing - don't crash the task
        // This ensures the pipeline continues processing other events even if one fails
        // For truly fatal errors, Flink's checkpointing and monitoring will detect issues
    }
  }

}
