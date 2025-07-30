package org.shikshalokam.job.user.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import scala.language.postfixOps

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def eventType: String = readOrDefault[String]("eventType", null)

  def userId: Int = readOrDefault[Int]("entityId", -1)

  def tenantCode: String = extractValue[String]("tenant_code").orNull

  def username: String = extractValue[String]("username").orNull

  def name: String = extractValue[String]("name").orNull

  def status: String = extractValue[String]("status").orNull

  def isDeleted: Boolean = extractValue[Boolean]("deleted").getOrElse(false)

  def createdBy: Int = extractValue[Int]("created_by").getOrElse(-1)

  def createdAt: Timestamp = parseTimestamp(extractValue[Any]("created_at").orNull)

  def updatedAt: Timestamp = parseTimestamp(extractValue[Any]("updated_at").orNull)

  def userProfileOneId: String = extractValue[String]("state.id").orNull

  def userProfileOneName: String = extractValue[String]("state.name").orNull

  def userProfileOneExternalId: String = extractValue[String]("state.externalId").orNull

  def userProfileTwoId: String = extractValue[String]("district.id").orNull

  def userProfileTwoName: String = extractValue[String]("district.name").orNull

  def userProfileTwoExternalId: String = extractValue[String]("district.externalId").orNull

  def userProfileThreeId: String = extractValue[String]("block.id").orNull

  def userProfileThreeName: String = extractValue[String]("block.name").orNull

  def userProfileThreeExternalId: String = extractValue[String]("block.externalId").orNull

  def userProfileFourId: String = extractValue[String]("cluster.id").orNull

  def userProfileFourName: String = extractValue[String]("cluster.name").orNull

  def userProfileFourExternalId: String = extractValue[String]("cluster.externalId").orNull

  def userProfileFiveId: String = extractValue[String]("school.id").orNull

  def userProfileFiveName: String = extractValue[String]("school.name").orNull

  def userProfileFiveExternalId: String = extractValue[String]("school.externalId").orNull

  def organizations: List[Map[String, Any]] = {
    val fromDefault = Option(readOrDefault[List[Map[String, Any]]]("organizations", null)).getOrElse(List.empty)

    var fromNew: List[Map[String, Any]] = List.empty
    var fromOld: List[Map[String, Any]] = List.empty

    if (eventType == "update" || eventType == "bulk-update") {
      fromNew = Option(readOrDefault[List[Map[String, Any]]]("newValues.organizations", null)).getOrElse(List.empty)
      fromOld = Option(readOrDefault[List[Map[String, Any]]]("oldValues.organizations", null)).getOrElse(List.empty)
    }

    (fromDefault ++ fromOld ++ fromNew).distinct
  }

  def professionalRoleId: String = extractNestedValue[String]("professional_role", "id").orNull

  def professionalRoleName: String = extractNestedValue[String]("professional_role", "name").orNull

  def professionalSubroles: List[Map[String, Any]] = {
    val defaultList = Option(readOrDefault[List[Map[String, Any]]]("professional_subroles", null)).getOrElse(List.empty)
    val newList = if (isUpdateEvent) Option(readOrDefault[List[Map[String, Any]]]("newValues.professional_subroles", null)).getOrElse(List.empty) else List.empty
    val oldList = if (isUpdateEvent) Option(readOrDefault[List[Map[String, Any]]]("oldValues.professional_subroles", null)).getOrElse(List.empty) else List.empty

    (defaultList ++ oldList ++ newList).distinct
  }

  private def extractValue[T](key: String): Option[T] = {
    val direct = Option(readOrDefault[T](key, null.asInstanceOf[T]))

    var fromNew: Option[T] = None
    var fromOld: Option[T] = None

    if (eventType == "update" || eventType == "bulk-update") {
      fromNew = Option(readOrDefault[T](s"newValues.$key", null.asInstanceOf[T]))
      fromOld = Option(readOrDefault[T](s"oldValues.$key", null.asInstanceOf[T]))
    }

    (direct orElse fromNew orElse fromOld).filter(_ != null)
  }

  private def isUpdateEvent: Boolean = eventType == "update" || eventType == "bulk-update"

  private def extractNestedValue[T](base: String, key: String): Option[T] = {
    val defaultMap = Option(readOrDefault[Map[String, Any]](base, null)).getOrElse(Map.empty)
    val newMap = if (isUpdateEvent) Option(readOrDefault[Map[String, Any]](s"newValues.$base", null)).getOrElse(Map.empty) else Map.empty
    val oldMap = if (isUpdateEvent) Option(readOrDefault[Map[String, Any]](s"oldValues.$base", null)).getOrElse(Map.empty) else Map.empty

    val combined = defaultMap ++ oldMap ++ newMap
    Option(combined.getOrElse(key, null).asInstanceOf[T]).filter(_ != null)
  }

  private def parseTimestamp(value: Any): Timestamp = value match {
    case ts: Timestamp => ts
    case s: String if s.trim.nonEmpty =>
      try {
        Timestamp.valueOf(s)
      } catch {
        case _: IllegalArgumentException =>
          try {
            Timestamp.from(Instant.parse(s))
          } catch {
            case _: Exception =>
              val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              new Timestamp(formatter.parse(s).getTime)
          }
      }
    case _ => new Timestamp(System.currentTimeMillis())
  }

}

