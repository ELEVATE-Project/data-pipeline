package org.shikshalokam.job.mentoring.stream.processor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.mentoring.stream.processor.domain.Event
import org.shikshalokam.job.mentoring.stream.processor.task.MentoringStreamConfig
import org.shikshalokam.job.util.{PostgresUtil, ScalaJsonUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer

class MentoringStreamFunction(config: MentoringStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MentoringStreamFunction])

  override def metricsList(): List[String] = {
    List(config.mentoringCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
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

    println(s"***************** Start of Processing Entity = ${event.entity} of Type = ${event.eventType} *****************")

    val tenantCode = event.tenantCode
    val eventType = event.eventType
    val entity = event.entity
    val name = event.name
    val status = event.status
    val createdBy = event.createdBy
    val updatedBy = event.updatedBy

    val createdAt = event.createdAt
    val updatedAt = event.updatedAt
    val deletedAt = event.deletedAt
    val isDeleted = event.isDeleted

    val sessionId = event.sessionId
    val mentorId = event.mentorId
    val sessionName = event.sessionName
    val sessionDesc = event.sessionDesc
    val sessionType = event.sessionType
    val sessionStatus = event.sessionStatus
    val platform = event.platform
    val startedAt = event.startedAt
    val completedAt = event.completedAt
    val startDate = event.startDate
    val endDate = event.endDate
    val recommendedFor = event.recommendedFor
    val categories = event.categories
    val medium = event.medium

    val attendanceId = event.attendanceId
    val attendanceSessionId = event.attendanceSessionId
    val menteeId = event.menteeId
    val joinedAt = event.joinedAt
    val leftAt = event.leftAt
    val isFeedbackSkipped = event.isFeedbackSkipped

    val connectionId = event.connectionId
    val userId = event.userId
    val friendId = event.friendId
    val orgId = event.orgId
    val orgCode = event.orgCode
    val orgName = event.orgName

    val rating = event.rating
    val ratingUpdatedAt = event.ratingUpdatedAt

    val tenantSessionTable: String = s""""${tenantCode}_sessions""""
    val tenantSessionAttendanceTable: String = s""""${tenantCode}_session_attendance""""
    val tenantConnectionsTable: String = s""""${tenantCode}_connections""""
    val tenantOrgMentorRatingTable: String = s""""${tenantCode}_org_mentor_rating""""

    println(s"tenantCode: $tenantCode")
    println(s"eventType: $eventType")
    println(s"entity: $entity")
    println(s"name: $name")
    println(s"status: $status")
    println(s"createdBy: $createdBy")
    println(s"updatedBy: $updatedBy")

    println(s"createdAt: $createdAt")
    println(s"updatedAt: $updatedAt")
    println(s"deletedAt: $deletedAt")
    println(s"isDeleted: $isDeleted")

    println(s"sessionId: $sessionId")
    println(s"mentorId: $mentorId")
    println(s"sessionName: $sessionName")
    println(s"description: $sessionDesc")
    println(s"sessionType: $sessionType")
    println(s"sessionStatus: $sessionStatus")
    println(s"platform: $platform")
    println(s"startedAt: $startedAt")
    println(s"completedAt: $completedAt")
    println(s"startDate: $startDate")
    println(s"endDate: $endDate")
    println(s"recommendedFor: $recommendedFor")
    println(s"categories: $categories")
    println(s"medium: $medium")

    println(s"attendanceId: $attendanceId")
    println(s"attendanceSessionId: $attendanceSessionId")
    println(s"menteeId: $menteeId")
    println(s"joinedAt: $joinedAt")
    println(s"leftAt: $leftAt")
    println(s"isFeedbackSkipped: $isFeedbackSkipped")

    println(s"connectionId: $connectionId")
    println(s"userId: $userId")
    println(s"friendId: $friendId")
    println(s"orgId: $orgId")
    println(s"orgCode: $orgCode")
    println(s"orgName: $orgName")

    println(s"rating: $rating")
    println(s"ratingUpdatedAt: $ratingUpdatedAt")

    val compareDashboardSessionTableFilters: List[Map[String, String]] = List(
      Map(
        "org_name" -> orgName,
        "table_name" -> tenantSessionTable
      )
    )

    def checkAndCreateTable(tableName: String, createTableQuery: String): Unit = {
      val checkTableExistsQuery =
        s"""SELECT EXISTS (
           |  SELECT FROM information_schema.tables
           |  WHERE table_name = '$tableName'
           |);
           |""".stripMargin

      val tableExists = postgresUtil.executeQuery(checkTableExistsQuery) { resultSet =>
        if (resultSet.next()) resultSet.getBoolean(1) else false
      }

      if (!tableExists) {
        postgresUtil.createTable(createTableQuery, tableName)
      }
    }

    val safeMentorId = Option(mentorId).map(_.toString.toInt).getOrElse(0)
    val safeOrgId = Option(orgId).map(_.toString.toInt).getOrElse(0)
    val safeCreatedBy = Option(createdBy).map(_.toString.toInt).getOrElse(0)
    val safeUpdatedBy = Option(updatedBy).map(_.toString.toInt).getOrElse(0)


    if (tenantCode.nonEmpty) {
      if (eventType == "create" || eventType == "bulk-create" || eventType == "update" || eventType == "bulk-update") {
        if (entity == "session") {
          val createSessionTable = config.createTenantSessionTable.replace("@sessions", tenantSessionTable)
          checkAndCreateTable(tenantSessionTable, createSessionTable)

          val compareDashboardSessionTableFiltersList = ListBuffer[String]()
          compareDashboardSessionTableFilters.foreach { filterMap =>
            filterMap.foreach { case (key, value) =>
              compareDashboardSessionTableFiltersList += checkIfValueExists(tenantSessionTable, "org_name", orgName)
            }
          }
          val finalCompareDashboardSessionTableFilters: List[String] = compareDashboardSessionTableFiltersList.toList
          checkExistenceOfDataAndPushMessageToKafka(finalCompareDashboardSessionTableFilters, context, tenantSessionTable, event.tenantCode)

          val insertSessionQuery =
            s"""
               |INSERT INTO $tenantSessionTable (id, session_id, mentor_id, name, description, type, status, start_date, end_date, org_id, org_code, org_name, platform, started_at, completed_at, created_at, updated_at, deleted_at, recommended_for, categories, medium, created_by, updated_by)
               |VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               |ON CONFLICT (session_id) DO UPDATE SET
               |  mentor_id = EXCLUDED.mentor_id, name = EXCLUDED.name, description = EXCLUDED.description, type = EXCLUDED.type,
               |  status = EXCLUDED.status, start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date, org_id = EXCLUDED.org_id, org_code = EXCLUDED.org_code, org_name = EXCLUDED.org_name,
               |  platform = EXCLUDED.platform, started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at,
               |  created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, deleted_at = EXCLUDED.deleted_at,
               |  recommended_for = EXCLUDED.recommended_for, categories = EXCLUDED.categories, medium = EXCLUDED.medium,
               |  created_by = EXCLUDED.created_by, updated_by = EXCLUDED.updated_by
               |""".stripMargin

          val sessionParams = Seq(sessionId, safeMentorId, sessionName, sessionDesc, sessionType, sessionStatus, startDate, endDate, safeOrgId, orgCode, orgName, platform, startedAt, completedAt, createdAt, updatedAt, deletedAt, recommendedFor, categories, medium, safeCreatedBy, safeUpdatedBy)
          postgresUtil.executePreparedUpdate(insertSessionQuery, sessionParams, tenantSessionTable, sessionId.toString)
        } else if (entity == "attendance") {
          val createSessionAttendanceTable = config.createTenantSessionAttendanceTable.replace("@sessionAttendance", tenantSessionAttendanceTable)
          checkAndCreateTable(tenantSessionAttendanceTable, createSessionAttendanceTable)

          val insertSessionAttendanceQuery =
            s"""
               |INSERT INTO $tenantSessionAttendanceTable (id, attendance_id, session_id, mentee_id, joined_at, left_at, is_feedback_skipped, type, created_at, updated_at, deleted_at)
               |VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               |ON CONFLICT (attendance_id) DO UPDATE SET
               |  session_id = EXCLUDED.session_id, mentee_id = EXCLUDED.mentee_id, joined_at = EXCLUDED.joined_at,
               |  left_at = EXCLUDED.left_at, is_feedback_skipped = EXCLUDED.is_feedback_skipped, type = EXCLUDED.type,
               |  created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, deleted_at = EXCLUDED.deleted_at
               |""".stripMargin

          val sessionAttendanceParams = Seq(attendanceId, attendanceSessionId, menteeId.toInt, joinedAt, leftAt, isFeedbackSkipped, sessionType, createdAt, updatedAt, deletedAt)
          postgresUtil.executePreparedUpdate(insertSessionAttendanceQuery, sessionAttendanceParams, tenantSessionAttendanceTable, attendanceId.toString)
        } else if (entity == "rating") {
          val createOrgMentorRatingTable = config.createOrgMentorRatingTable.replace("@orgMentorRating", tenantOrgMentorRatingTable)
          checkAndCreateTable(tenantOrgMentorRatingTable, createOrgMentorRatingTable)

          val insertOrgMentorRatingQuery =
            s"""
               |INSERT INTO $tenantOrgMentorRatingTable (id, org_id, org_name, mentor_id, rating, rating_updated_at)
               |VALUES (DEFAULT, ?, ?, ?, ?, ?)
               |""".stripMargin

          val orgMentorRatingParams = Seq(orgId.toInt, orgName, mentorId.toInt, rating, ratingUpdatedAt)
          postgresUtil.executePreparedUpdate(insertOrgMentorRatingQuery, orgMentorRatingParams, tenantOrgMentorRatingTable, orgId.toString)
        } else if (entity == "connections") {
          val createConnectionsTable = config.createTenantConnectionsTable.replace("@connectionsTable", tenantConnectionsTable)
          checkAndCreateTable(tenantConnectionsTable, createConnectionsTable)

          val insertConnectionsQuery =
            s"""
               |INSERT INTO $tenantConnectionsTable (id, connection_id, user_id, friend_id, status, org_id, created_by, updated_by, created_at, updated_at, deleted_at)
               |VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               |ON CONFLICT (connection_id) DO UPDATE SET
               |  user_id = EXCLUDED.user_id, friend_id = EXCLUDED.friend_id, status = EXCLUDED.status,
               |  org_id = EXCLUDED.org_id, created_by = EXCLUDED.created_by, updated_by = EXCLUDED.updated_by,
               |  created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, deleted_at = EXCLUDED.deleted_at
               |""".stripMargin

          val connectionsParams = Seq(connectionId, userId.toInt, friendId.toInt, sessionStatus, orgId.toInt, createdBy.toInt, updatedBy.toInt, createdAt, updatedAt, deletedAt)
          postgresUtil.executePreparedUpdate(insertConnectionsQuery, connectionsParams, tenantConnectionsTable, connectionId.toString)
        }
      } else if (eventType == "delete") {
        println(s"Processing delete event for entity: $entity")
        if (entity == "session") {
          deleteSessionAndAttendance(tenantSessionTable, tenantSessionAttendanceTable, sessionId, deletedAt)
        } else if (entity == "connections") {
          val deleteConnectionsQuery =
            s"""
               |UPDATE $tenantConnectionsTable
               |SET deleted_at = ?, status = 'DELETED'
               |WHERE connection_id = ?
         """.stripMargin
          println(s"Connection record for connectionId: $connectionId is Deleted for table $tenantConnectionsTable")
          postgresUtil.executePreparedUpdate(deleteConnectionsQuery, Seq(deletedAt, connectionId), tenantConnectionsTable, connectionId.toString)
        }
      }
    } else {
      println("Tenant code is empty, skipping processing.")
    }

    def deleteSessionAndAttendance(tenantSessionTable: String, tenantSessionAttendanceTable: String, sessionId: Int, deletedAt: java.sql.Timestamp): Unit = {
      try {
        val deleteSessionQuery =
          s"""
             |UPDATE $tenantSessionTable
             |SET status = 'DELETED', deleted_at = ?
             |WHERE session_id = ?
        """.stripMargin
        postgresUtil.executePreparedUpdate(deleteSessionQuery, Seq(deletedAt, sessionId), tenantSessionTable, sessionId.toString)
        println(s"Session $sessionId marked as deleted in table: $tenantSessionTable")

        val attendanceTable = tenantSessionAttendanceTable.replaceAll("\"", "")
        val checkAttendanceTableExistsQuery =
          s"""
             |SELECT EXISTS (
             |  SELECT 1 FROM information_schema.tables
             |  WHERE LOWER(table_name) = LOWER('$attendanceTable')
             |);
          """.stripMargin
        val attendanceTableExists = postgresUtil.executeQuery(checkAttendanceTableExistsQuery) { resultSet =>
          if (resultSet.next()) resultSet.getBoolean(1) else false
        }

        if (attendanceTableExists) {
          val deleteAttendanceQuery =
            s"""
               |UPDATE $tenantSessionAttendanceTable
               |SET type = 'DELETED', deleted_at = ?
               |WHERE session_id = ?
        """.stripMargin

          postgresUtil.executePreparedUpdate(deleteAttendanceQuery, Seq(deletedAt, sessionId), tenantSessionAttendanceTable, sessionId.toString)
          println(s"Attendance records for session $sessionId marked as deleted in table: $tenantSessionAttendanceTable")
        } else {
          println(s"Table $tenantSessionAttendanceTable does not exist — skipping attendance delete.")
        }

      } catch {
        case ex: Exception =>
          println(s"Error while deleting sessionId $sessionId: ${ex.getMessage}")
      }
    }

    /**
     * Logic to populate kafka messages for creating user metabase dashboard
     */
    postgresUtil.createTable(config.createDashboardMetadataTable, config.dashboardMetadata)

    val dashboardData = new java.util.HashMap[String, String]()
    val dashboardConfig = Seq(
      ("Mentoring", s"${event.tenantCode}_Mentoring_Tenant_Admin_Dashboard", s"${event.tenantCode}_tenant_admin"), // Tenant Admin
      ("Mentoring", s"${event.tenantCode}_Mentoring_Org_Admin_Dashboard", s"org_admin_${event.orgId}") // Org Admin
    )

    dashboardConfig.foreach { case (entityType, entityName, entityId) =>
      checkAndInsert(entityType, entityName, entityId, dashboardData)
    }

    if (!dashboardData.isEmpty) {
      pushMentoringDashboardEvents(dashboardData, context)
    }

    println(s"***************** Completed Processing Entity = ${event.entity} of Type = ${event.eventType}  *****************")

    def checkAndInsert(entityType: String, entityName: String, entityId: String, dashboardData: java.util.HashMap[String, String]): Unit = {
      if (tenantCode.isEmpty || orgId == null) {
        println(s"Tenant code or Org Id is empty, skipping insertion.")
        return
      }
      val query = s"SELECT EXISTS (SELECT 1 FROM ${config.dashboardMetadata} WHERE entity_type = '$entityType' AND entity_id = '$entityId') AS is_present"
      val result = postgresUtil.fetchData(query)

      result.foreach { row =>
        row.get(s"is_present") match {
          case Some(isPresent: Boolean) if isPresent =>
            println(s"$entityType details already exist.")
          case _ =>
            if (entityType == "Mentoring") {
              val insertQuery = s"INSERT INTO ${config.dashboardMetadata} (entity_type, entity_name, entity_id) VALUES ('$entityType', '$entityName', '$entityId')"
              val affectedRows = postgresUtil.insertData(insertQuery)
              println(s"Inserted mentoringDashboard details. Affected rows: $affectedRows")
              dashboardData.put("tenantCode", event.tenantCode)
              dashboardData.put("orgId", event.orgId)
              dashboardData.put("orgName", event.orgName)
            }
        }
      }
    }

  }

  private def checkIfValueExists(tableName: String, columnName: String, value: String)(implicit postgresUtil: PostgresUtil): String = {
    if (value == null || value.trim.isEmpty) {
      return ""
    }
    val rowCountQuery = s"SELECT COUNT(*) AS row_count FROM $tableName"
    val rowCount = postgresUtil.executeQuery(rowCountQuery) { rs =>
      if (rs.next()) rs.getLong("row_count") else 0
    }
    if (rowCount == 0) {
      println(s"Table $tableName has no rows → first time inserting data.")
      ""
    } else {
      val safeValue = value.replace("'", "''")
      val query =
        s"""
           |SELECT
           |    CASE
           |        WHEN EXISTS (
           |            SELECT 1
           |            FROM $tableName
           |            WHERE $columnName = '$safeValue'
           |        )
           |        THEN 'Yes'
           |        ELSE 'No'
           |    END AS exists_flag;
           |""".stripMargin

      postgresUtil.executeQuery(query) { resultSet =>
        if (resultSet.next()) resultSet.getString("exists_flag") else ""
      }
    }
  }

  private def checkExistenceOfDataAndPushMessageToKafka(resultList: List[String], context: ProcessFunction[Event, Event]#Context, tableName: String, tenantCode: String): Unit = {
    if (resultList.exists(s => s == null || s.trim.isEmpty)) {
      return
    }
    if (resultList.contains("No")) {
      val eventData = new java.util.HashMap[String, String]()
      eventData.put("filterTable", tableName.stripPrefix("\"").stripSuffix("\""))
      eventData.put("filterSync", "Yes")
      pushMentoringDashboardEvents(eventData, context)
      println(s"eventData: $eventData")
    } else {
      println(s"Data already Exists in $tableName → not sending Kafka message for $tableName")
    }
  }

  private def pushMentoringDashboardEvents(dashboardData: util.HashMap[String, String], context: ProcessFunction[Event, Event]#Context): util.HashMap[String, AnyRef] = {
    val objects = new util.HashMap[String, AnyRef]() {
      put("_id", java.util.UUID.randomUUID().toString)
      put("reportType", "Mentoring")
      put("publishedAt", DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(ZoneId.systemDefault())
        .format(Instant.ofEpochMilli(System.currentTimeMillis())).asInstanceOf[AnyRef])
      put("dashboardData", dashboardData)
    }

    val serializedEvent = ScalaJsonUtil.serialize(objects)
    context.output(config.eventOutputTag, serializedEvent)
    println(s"----> Pushed new Kafka message to ${config.outputTopic} topic")
    println(objects)
    objects
  }
}