package org.shikshalokam.job.user.mapping.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest
import ujson.Js

import java.util
import scala.collection.JavaConverters._

/**
 * Case class to represent observation submission events from Kafka
 * 
 * Sample event structure:
 * {
 *   "eventType": "observation-submitted",
 *   "id": 3088,
 *   "organizationId": 1,
 *   "observationData": {
 *     "name": "Carol Miranda Updated Two",
 *     "about": "admin Update",
 *     "dob": "22-12-1990"
 *   }
 * }
 */
class ObservationEvent(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) 
  extends JobRequest(eventMap, partition, offset) {

  def eventType: String = readOrDefault[String]("eventType", null)
  
  def id: String = {
    // Priority order: userProfile.id > entityId > id (root) > oldValues.id/newValues.id
    
    // 1. Try userProfile.id first (highest priority)
    val userProfileId = readOrDefault[Any]("userProfile.id", null)
    if (userProfileId != null) {
      return convertToString(userProfileId)
    }
    
    // 2. Try entityId at root level
    val entityIdValue = readOrDefault[Any]("entityId", null)
    if (entityIdValue != null) {
      return convertToString(entityIdValue)
    }
    
    // 3. Try id at root level
    val idValue = readOrDefault[Any]("id", null)
    if (idValue != null) {
      return convertToString(idValue)
    }
    
    // 4. For update events, try oldValues.id or newValues.id
    if (eventType == "update" || eventType == "bulk-update") {
      val newValuesId = readOrDefault[Any]("newValues.id", null)
      if (newValuesId != null) {
        return convertToString(newValuesId)
      }
      
      val oldValuesId = readOrDefault[Any]("oldValues.id", null)
      if (oldValuesId != null) {
        return convertToString(oldValuesId)
      }
    }
    
    null
  }
  
  private def convertToString(value: Any): String = {
    if (value == null) return null
    value match {
      case n: Number => n.toString
      case s: String => s
      case _ => value.toString
    }
  }
  
  def organizationId: Long = {
    val value = readOrDefault[Any]("organizationId", null)
    value match {
      case l: Long => l
      case i: Int => i.toLong
      case n: Number => n.longValue()
      case _ => -1L
    }
  }
  
  def observationData: util.Map[String, Any] = {
    val userProfile = readOrDefault[Any]("userProfile", null)
    if (userProfile != null) {
      userProfile match {
        case javaMap: util.Map[_, _] => javaMap.asInstanceOf[util.Map[String, Any]]
        case scalaMap: scala.collection.Map[_, _] => scalaMap.asInstanceOf[scala.collection.Map[String, Any]].asJava
        case _ => new util.HashMap[String, Any]()
      }
    } else {
      new util.HashMap[String, Any]()
    }
  }
  
  override def toString: String = {
    s"ObservationEvent(eventType=$eventType, id=$id, organizationId=$organizationId)"
  }
}
