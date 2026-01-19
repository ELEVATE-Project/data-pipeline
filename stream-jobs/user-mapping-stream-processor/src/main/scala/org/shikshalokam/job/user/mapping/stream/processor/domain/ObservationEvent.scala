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
  
  def studentId: String = {
    // Support both "id" (numeric) and "studentId" (string) for backward compatibility
    val idValue = readOrDefault[Any]("id", null)
    val studentIdValue = readOrDefault[String]("studentId", null)
    
    if (idValue != null) {
      // Convert numeric ID to string
      idValue match {
        case n: Number => n.toString
        case s: String => s
        case _ => idValue.toString
      }
    } else if (studentIdValue != null) {
      studentIdValue
    } else {
      null
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
    val data = readOrDefault[Any]("observationData", null)
    if (data == null) {
      new util.HashMap[String, Any]()
    } else {
      data match {
        case javaMap: util.Map[String, Any] => javaMap
        case scalaMap: scala.collection.Map[String, Any] => scalaMap.asJava
        case _ => new util.HashMap[String, Any]()
      }
    }
  }
  
  override def toString: String = {
    s"ObservationEvent(eventType=$eventType, studentId=$studentId, organizationId=$organizationId)"
  }
}
